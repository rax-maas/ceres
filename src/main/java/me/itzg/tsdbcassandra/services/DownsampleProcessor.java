package me.itzg.tsdbcassandra.services;

import static me.itzg.tsdbcassandra.downsample.ValueSetCollectors.counterCollector;
import static me.itzg.tsdbcassandra.downsample.ValueSetCollectors.gaugeCollector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import me.itzg.tsdbcassandra.config.DownsampleProperties;
import me.itzg.tsdbcassandra.config.DownsampleProperties.Granularity;
import me.itzg.tsdbcassandra.config.IntegerSet;
import me.itzg.tsdbcassandra.config.StringToIntegerSetConverter;
import me.itzg.tsdbcassandra.downsample.AggregatedValueSet;
import me.itzg.tsdbcassandra.downsample.Aggregator;
import me.itzg.tsdbcassandra.downsample.DataDownsampled;
import me.itzg.tsdbcassandra.downsample.TemporalNormalizer;
import me.itzg.tsdbcassandra.downsample.ValueSet;
import me.itzg.tsdbcassandra.model.PendingDownsampleSet;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

@Service
@Slf4j
public class DownsampleProcessor {

  private final Environment env;
  private final ObjectMapper objectMapper;
  private final DownsampleProperties downsampleProperties;
  private final DownsampleTrackingService downsampleTrackingService;
  private final TaskScheduler taskScheduler;
  private final SeriesSetService seriesSetService;
  private final QueryService queryService;
  private final DataWriteService dataWriteService;
  private List<ScheduledFuture<?>> scheduled;

  @Autowired
  public DownsampleProcessor(Environment env,
                             ObjectMapper objectMapper,
                             DownsampleProperties downsampleProperties,
                             DownsampleTrackingService downsampleTrackingService,
                             @Qualifier("downsampleTaskScheduler") TaskScheduler taskScheduler,
                             SeriesSetService seriesSetService,
                             QueryService queryService,
                             DataWriteService dataWriteService) {
    this.env = env;
    this.objectMapper = objectMapper;
    this.downsampleProperties = downsampleProperties;
    this.downsampleTrackingService = downsampleTrackingService;
    this.taskScheduler = taskScheduler;
    this.seriesSetService = seriesSetService;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
  }

  @PostConstruct
  public void setupSchedulers() {
    if (env.acceptsProfiles(Profiles.of("test"))) {
      log.warn("Downsample scheduling disabled during testing");
      return;
    }

    final IntegerSet partitionsToProcess = getPartitionsToProcess();
    if (partitionsToProcess == null) {
      // just info level since this is the normal way to disable downsampling
      log.info("Downsample processing is disabled due to no partitions to process");
      return;
    }

    if (downsampleProperties.getGranularities() == null ||
        downsampleProperties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Downsample partitions to process are configured, but not granularities");
    }

    scheduled = partitionsToProcess.stream()
        .map(partition ->
            taskScheduler.scheduleAtFixedRate(
                () -> processPartition(partition),
                Instant.now()
                    .plus(randomizeInitialDelay()),
                downsampleProperties.getDownsampleProcessPeriod()
            )
        )
        .collect(Collectors.toList());

    log.debug("Downsample processing is scheduled");
  }

  private IntegerSet getPartitionsToProcess() {
    if (downsampleProperties.getPartitionsToProcess() != null &&
        !downsampleProperties.getPartitionsToProcess().isEmpty()) {
      return downsampleProperties.getPartitionsToProcess();
    }

    if (downsampleProperties.getPartitionsMappingFile() != null) {
      try {
        final Map<String, String> mappings = objectMapper.readValue(
            downsampleProperties.getPartitionsMappingFile().toFile(),
            new TypeReference<>() {}
        );

        final String ourHostname = InetAddress.getLocalHost().getHostName();
        final String entry = mappings.get(ourHostname);

        if (entry != null) {
          log.debug("Loaded partitions to process for {} from {}",
              ourHostname, downsampleProperties.getPartitionsMappingFile());
          return new StringToIntegerSetConverter().convert(entry);
        } else {
          throw new IllegalStateException(String.format(
              "Unable to locate partitions to process for %s in %s",
                  ourHostname, downsampleProperties.getPartitionsMappingFile()
              ));
        }
      } catch (IOException e) {
        throw new IllegalStateException("Unable to read partitions mapping file", e);
      }
    }

    return null;
  }

  private Duration randomizeInitialDelay() {
    return downsampleProperties.getInitialProcessingDelay()
        .plus(
            downsampleProperties.getInitialProcessingDelay().dividedBy(
                2 + new Random().nextInt(8)
            )
        );
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
