package me.itzg.tsdbcassandra.services;

import static me.itzg.tsdbcassandra.downsample.ValueSetCollectors.counterCollector;
import static me.itzg.tsdbcassandra.downsample.ValueSetCollectors.gaugeCollector;

import java.time.Instant;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.PostConstruct;
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
import org.springframework.boot.task.TaskSchedulerBuilder;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

@Service
@Slf4j
public class DownsampleProcessor {

  private final DownsampleProperties downsampleProperties;
  private final DownsampleTrackingService downsampleTrackingService;
  private final SeriesSetService seriesSetService;
  private final QueryService queryService;
  private final IngestService ingestService;
  private final MetadataService metadataService;

  @Autowired
  public DownsampleProcessor(DownsampleProperties downsampleProperties,
                             DownsampleTrackingService downsampleTrackingService,
                             SeriesSetService seriesSetService,
                             QueryService queryService,
                             IngestService ingestService,
                             MetadataService metadataService) {
    this.downsampleProperties = downsampleProperties;
    this.downsampleTrackingService = downsampleTrackingService;
    this.seriesSetService = seriesSetService;
    this.queryService = queryService;
    this.ingestService = ingestService;
    this.metadataService = metadataService;
  }

  @PostConstruct
  public void setupSchedulers() {
    final ThreadPoolTaskScheduler scheduler = new TaskSchedulerBuilder()
        .threadNamePrefix("downsample-")
        .poolSize(Runtime.getRuntime().availableProcessors())
        .build();

    for (int partition : downsampleProperties.expandPartitionsToProcess()) {
      scheduler.scheduleAtFixedRate(
          () -> process(partition),
          downsampleProperties.getDownsampleProcessPeriod()
      );
    }
  }

  private void process(int partition) {
    log.debug("Downsampling partition {}", partition);

    downsampleTrackingService
        .retrieveReadyOnes(partition)
        .flatMap(this::processDownsampleSet)
        .subscribe();
  }

  private Publisher<?> processDownsampleSet(PendingDownsampleSet pendingDownsampleSet) {
    final boolean isCounter = seriesSetService.isCounter(pendingDownsampleSet.getSeriesSet());

    final Flux<ValueSet> data = queryService.queryRaw(
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

    return Mono.from(aggregated)
        .and(
            metadataService.updateMetricNames(
                pendingDownsampleSet.getTenant(),
                seriesSetService.metricNameFromSeriesSet(pendingDownsampleSet.getSeriesSet()),
                isCounter ? Set.of(Aggregator.sum) : Set.of(Aggregator.sum, Aggregator.min, Aggregator.max, Aggregator.avg)
            )
        )
        .and(
            downsampleTrackingService.complete(pendingDownsampleSet)
        );
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
        ingestService.storeDownsampledData(expanded, granularity.getTtl())
            .concatWith(
                // ...and recurse into remaining granularities
                aggregateData(aggregated, tenant, seriesSet, granularities, isCounter)
            );
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