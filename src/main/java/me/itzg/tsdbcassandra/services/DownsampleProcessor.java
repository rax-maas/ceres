package me.itzg.tsdbcassandra.services;

import static me.itzg.tsdbcassandra.downsample.ValueSetCollectors.counterCollector;
import static me.itzg.tsdbcassandra.downsample.ValueSetCollectors.gaugeCollector;

import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import me.itzg.tsdbcassandra.config.DownsampleProperties;
import me.itzg.tsdbcassandra.config.DownsampleProperties.Granularity;
import me.itzg.tsdbcassandra.downsample.AggregatedValueSet;
import me.itzg.tsdbcassandra.downsample.TemporalNormalizer;
import me.itzg.tsdbcassandra.downsample.ValueSet;
import me.itzg.tsdbcassandra.entities.Aggregator;
import me.itzg.tsdbcassandra.entities.DataDownsampled;
import me.itzg.tsdbcassandra.entities.PendingDownsampleSet;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@Service
@Slf4j
public class DownsampleProcessor {

  private final Environment env;
  private final DownsampleProperties downsampleProperties;
  private final DownsampleTrackingService downsampleTrackingService;
  private final TaskScheduler taskScheduler;
  private final SeriesSetService seriesSetService;
  private final QueryService queryService;
  private final DataWriteService dataWriteService;
  private final MetadataService metadataService;
  private List<ScheduledFuture<?>> scheduled;

  @Autowired
  public DownsampleProcessor(Environment env,
                             DownsampleProperties downsampleProperties,
                             DownsampleTrackingService downsampleTrackingService,
                             @Qualifier("downsampleTaskScheduler") TaskScheduler taskScheduler,
                             SeriesSetService seriesSetService,
                             QueryService queryService,
                             DataWriteService dataWriteService,
                             MetadataService metadataService) {
    this.env = env;
    this.downsampleProperties = downsampleProperties;
    this.downsampleTrackingService = downsampleTrackingService;
    this.taskScheduler = taskScheduler;
    this.seriesSetService = seriesSetService;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
    this.metadataService = metadataService;
  }

  @PostConstruct
  public void setupSchedulers() {
    if (env.acceptsProfiles(Profiles.of("test"))) {
      log.warn("Downsample scheduling disabled during testing");
      return;
    }

    if (downsampleProperties.getPartitionsToProcess() == null ||
        downsampleProperties.getPartitionsToProcess().isEmpty()) {
      // just info level since this is the normal way to disable downsampling
      log.info("Downsample processing is disabled due to no partitions to process");
      return;
    }

    if (downsampleProperties.getGranularities() == null ||
        downsampleProperties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Downsample partitions to process are configured, but not granularities");
    }

    scheduled = downsampleProperties.getPartitionsToProcess().stream()
        .map(partition ->
            taskScheduler.scheduleAtFixedRate(
                () -> processPartition(partition),
                downsampleProperties.getDownsampleProcessPeriod()
            )
        )
        .collect(Collectors.toList());

    log.debug("Downsample processing is scheduled");
  }

  @PreDestroy
  public void stop() {
    if (scheduled != null) {
      scheduled.forEach(scheduledFuture -> scheduledFuture.cancel(false));
    }
  }

  private void processPartition(int partition) {
    log.trace("Downsampling partition {}", partition);

    downsampleTrackingService
        .retrieveReadyOnes(partition)
        // Each retrieval above does so with a row count limit, so the following requests
        // a repeated subscription when the number of retrieved downsample sets is more than zero
        .repeatWhen(retrievedCountFlux ->
            retrievedCountFlux.flatMap(retrievedCount -> {
              log.trace("Retrieved count={} pending downsample sets for partition={}", retrievedCount, partition);
              return retrievedCount > 0 ?
                  // add a slight delay in between each repetition to avoid saturating cassandra
                  // with downsample updates given a large number of pending downsample sets
                  Mono.delay(downsampleProperties.getPendingRetrieveRepeatDelay())
                  : Mono.<Long>empty();
            }))
        .flatMap(this::processDownsampleSet)
        .doOnError(throwable -> log.warn("Failed to process partition={}", partition, throwable))
        .subscribe();
  }

  private Publisher<?> processDownsampleSet(PendingDownsampleSet pendingDownsampleSet) {
    log.debug("Processing downsample set {}", pendingDownsampleSet);

    final boolean isCounter = seriesSetService.isCounter(pendingDownsampleSet.getSeriesSet());

    final Flux<ValueSet> data = queryService.queryRawWithSeriesSet(
        pendingDownsampleSet.getTenant(),
        pendingDownsampleSet.getSeriesSet(),
        pendingDownsampleSet.getTimeSlot(),
        pendingDownsampleSet.getTimeSlot().plus(downsampleProperties.getTimeSlotWidth())
    );

    final Flux<Tuple2<DataDownsampled, Boolean>> aggregated = aggregateData(data,
        pendingDownsampleSet.getTenant(),
        pendingDownsampleSet.getSeriesSet(),
        downsampleProperties.getGranularities().iterator(), isCounter
    );

    return
        aggregated
            .name("downsample")
            .metrics()
            .then(
                metadataService.updateMetricNames(
                    pendingDownsampleSet.getTenant(),
                    seriesSetService.metricNameFromSeriesSet(pendingDownsampleSet.getSeriesSet()),
                    isCounter ? Set.of(Aggregator.sum) : Set.of(Aggregator.sum, Aggregator.min, Aggregator.max, Aggregator.avg)
                )
            )
            .then(
                downsampleTrackingService.complete(pendingDownsampleSet)
            )
            .doOnError(throwable -> log.warn("Failed to downsample {}", pendingDownsampleSet, throwable))
            .doOnSuccess(o -> log.debug("Completed downsampling of {}", pendingDownsampleSet))
            .checkpoint();
  }

  /**
   * Aggregates the given data into the next granularity, schedules the storage of that data,
   * and recurses until the remaining granularities are processed.
   * @param data a flux of either raw {@link me.itzg.tsdbcassandra.downsample.SingleValueSet}s or
   * aggregated {@link AggregatedValueSet}s from the prior granularity.
   * @param tenant the tenant of the pending downsample set
   * @param seriesSet the series-set of the pending downsample set
   * @param granularities remaining granularties to process
   * @param isCounter indicates if the original metric is a counter or gauge
   * @return a flux of the stored downsamples along with the "applied" indicator from Cassandra
   */
  public Flux<Tuple2<DataDownsampled, Boolean>> aggregateData(Flux<? extends ValueSet> data,
                                                              String tenant,
                                                              String seriesSet,
                                                              Iterator<Granularity> granularities,
                                                              boolean isCounter) {
    if (!granularities.hasNext()) {
      // end of the recursion so pop back out
      return Flux.empty();
    }

    final Granularity granularity = granularities.next();
    final TemporalNormalizer normalizer = new TemporalNormalizer(granularity.getWidth());

    final Flux<AggregatedValueSet> aggregated =
        data
            .doOnNext(valueSet -> log.trace("Aggregating {} into granularity={}", valueSet, granularity))
            // group the incoming data by granularity-time-window
            .windowUntilChanged(
                valueSet -> valueSet.getTimestamp().with(normalizer), Instant::equals)
            // ...and then do the aggregation math on those
            .concatMap(valueSetFlux -> valueSetFlux.collect(
                isCounter ? counterCollector(granularity.getWidth())
                    : gaugeCollector(granularity.getWidth())
            ));

    final Flux<DataDownsampled> expanded = expandAggregatedData(
        aggregated, tenant, seriesSet, isCounter);

    return
        // store this granularity of aggregated downsamples providing the TTL/retention configured
        // for this granularity
        dataWriteService.storeDownsampledData(expanded, granularity.getTtl())
            .concatWith(
                // ...and recurse into remaining granularities
                aggregateData(aggregated, tenant, seriesSet, granularities, isCounter)
            )
            .checkpoint();
  }

  public Flux<DataDownsampled> expandAggregatedData(Flux<AggregatedValueSet> aggs, String tenant,
                                                    String seriesSet, boolean isCounter) {
    return aggs.flatMap(agg -> {
      if (isCounter) {
        return Flux.just(
            data(tenant, seriesSet, agg).setAggregator(Aggregator.sum).setValue(agg.getSum())
        );
      } else {
        return Flux.just(
            data(tenant, seriesSet, agg).setAggregator(Aggregator.sum).setValue(agg.getSum()),
            data(tenant, seriesSet, agg).setAggregator(Aggregator.min).setValue(agg.getMin()),
            data(tenant, seriesSet, agg).setAggregator(Aggregator.max).setValue(agg.getMax()),
            data(tenant, seriesSet, agg).setAggregator(Aggregator.avg).setValue(agg.getAverage())
        );
      }
    });
  }

  private static DataDownsampled data(String tenant, String seriesSet, AggregatedValueSet agg) {
    return new DataDownsampled()
        .setTs(agg.getTimestamp())
        .setGranularity(agg.getGranularity())
        .setTenant(tenant)
        .setSeriesSet(seriesSet);
  }
}
