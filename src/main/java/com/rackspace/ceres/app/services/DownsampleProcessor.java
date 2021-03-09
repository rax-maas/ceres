/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.ceres.app.services;

import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.config.IntegerSet;
import com.rackspace.ceres.app.config.StringToIntegerSetConverter;
import com.rackspace.ceres.app.downsample.AggregatedValueSet;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.downsample.DataDownsampled;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.downsample.ValueSetCollectors;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
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
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Service;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@Profile("downsample")
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
        .map(partition -> {
          final Duration initialDelay = randomizeInitialDelay();
          log.debug("Scheduling partition={} after={} every={}",
              partition, initialDelay, downsampleProperties.getDownsampleProcessPeriod());
          return taskScheduler.scheduleAtFixedRate(
                  () -> processPartition(partition),
                  Instant.now()
                      .plus(initialDelay),
                  downsampleProperties.getDownsampleProcessPeriod()
              );
            }
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
            downsampleProperties.getDownsampleProcessPeriod().dividedBy(
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
        .subscribe(o -> {}, throwable -> {
          if (Exceptions.isRetryExhausted(throwable)) {
            throwable = throwable.getCause();
          }
          if (isNoNodeAvailable(throwable)) {
            log.warn("Failed to process partition={}: {}", partition, throwable.getMessage());
          } else {
            log.warn("Failed to process partition={}", partition, throwable);
          }
        });
  }

  private static boolean isNoNodeAvailable(Throwable throwable) {
    if (throwable instanceof CassandraConnectionFailureException) {
      return throwable.getCause() instanceof NoNodeAvailableException;
    }
    return false;
  }

  private Publisher<?> processDownsampleSet(PendingDownsampleSet pendingDownsampleSet) {
    log.trace("Processing downsample set {}", pendingDownsampleSet);

    final boolean isCounter = seriesSetService.isCounter(pendingDownsampleSet.getSeriesSetHash());

    final Flux<ValueSet> data = queryService.queryRawWithSeriesSet(
        pendingDownsampleSet.getTenant(),
        pendingDownsampleSet.getSeriesSetHash(),
        pendingDownsampleSet.getTimeSlot(),
        pendingDownsampleSet.getTimeSlot().plus(downsampleProperties.getTimeSlotWidth())
    );

    return
        downsampleData(data,
            pendingDownsampleSet.getTenant(),
            pendingDownsampleSet.getSeriesSetHash(),
            downsampleProperties.getGranularities().iterator(), isCounter
        )
            .name("downsample")
            .metrics()
            .then(
                downsampleTrackingService.complete(pendingDownsampleSet)
            )
            .doOnSuccess(o -> log.trace("Completed downsampling of {}", pendingDownsampleSet))
            .checkpoint();
  }

  /**
   * Downsamples the given data into the next granularity, stores that data,
   * and recurses until the remaining granularities are processed.
   * @param data a flux of either raw {@link SingleValueSet}s or
   * aggregated {@link AggregatedValueSet}s from the prior granularity.
   * @param tenant the tenant of the pending downsample set
   * @param seriesSet the series-set of the pending downsample set
   * @param granularities remaining granularties to process
   * @param isCounter indicates if the original metric is a counter or gauge
   * @return a mono that completes when the aggregated data has been stored
   */
  public Mono<?> downsampleData(Flux<? extends ValueSet> data,
                                String tenant,
                                String seriesSet,
                                Iterator<Granularity> granularities,
                                boolean isCounter) {
    if (!granularities.hasNext()) {
      // end of the recursion so pop back out
      return Mono.empty();
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
                isCounter ? ValueSetCollectors.counterCollector(granularity.getWidth())
                    : ValueSetCollectors.gaugeCollector(granularity.getWidth())
            ));

    // expand the aggregated volue-sets into individual data points to be stored
    final Flux<DataDownsampled> expanded = expandAggregatedData(
        aggregated, tenant, seriesSet, isCounter);

    return
        dataWriteService.storeDownsampledData(expanded)
            .then(
                // ...and recurse into remaining granularities
                downsampleData(aggregated, tenant, seriesSet, granularities, isCounter)
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
        .setSeriesSetHash(seriesSet);
  }
}
