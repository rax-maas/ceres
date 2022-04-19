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
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.*;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.CassandraConnectionFailureException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Service
@Slf4j
@Profile("downsample")
public class DownsampleProcessor {

  private final DownsampleProperties downsampleProperties;
  private final DownsampleTrackingService downsampleTrackingService;
  private final SeriesSetService seriesSetService;
  private final QueryService queryService;
  private final DataWriteService dataWriteService;
  private final Timer meterTimer;
  private final ScheduledExecutorService executor;

  @Autowired
  public DownsampleProcessor(DownsampleProperties downsampleProperties,
                             DownsampleTrackingService downsampleTrackingService,
                             SeriesSetService seriesSetService,
                             QueryService queryService,
                             DataWriteService dataWriteService,
                             MeterRegistry meterRegistry,
                             ScheduledExecutorService executor) {
    this.downsampleProperties = downsampleProperties;
    this.downsampleTrackingService = downsampleTrackingService;
    this.seriesSetService = seriesSetService;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
    this.meterTimer = meterRegistry.timer("downsampling.delay");
    this.executor = executor;
  }

  @PostConstruct
  public void setupSchedulers() {
    if (downsampleProperties.getGranularities() == null ||
        downsampleProperties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Granularities are not configured!");
    }
    executor.schedule(this::initializeRedisJobs, 1, TimeUnit.SECONDS);
    executor.schedule(
            this::initializeJobs, downsampleProperties.getInitialProcessingDelay().getSeconds(), TimeUnit.SECONDS);
  }

  @PreDestroy
  public void stop() {
    executor.shutdown();
  }

  private void initializeRedisJobs() {
    log.info("Initialize redis jobs...");
    IntStream.rangeClosed(0, downsampleProperties.getPartitions() - 1).forEach(this::initRedisJob);
  }

  private void initializeJobs() {
    log.info("Initialize downsampling jobs...");
    IntStream.rangeClosed(0, downsampleProperties.getPartitions() - 1).forEach((i) -> {
      executor.scheduleAtFixedRate(
              () -> processJob(i), new Random().nextInt(1000), 1000, TimeUnit.MILLISECONDS);
    });
  }

  private void initRedisJob(int partition) {
    downsampleTrackingService.initJob(partition).subscribe(o -> {}, throwable -> {});
  }

  private void processJob(int partition) {
    downsampleTrackingService.checkPartitionJob(partition)
            .flatMap(status -> {
              if (status.equals("free")) {
                return processTimeSlots(partition);
              } else {
                return Mono.empty();
              }
            }).subscribe(o -> {}, throwable -> {});
    }

  private Mono<?> processTimeSlots(int partition) {
    return downsampleTrackingService.retrieveTimeSlots(partition)
            .flatMap(downsampleSet ->  processDownsampleSet(downsampleSet, partition))
                    .then(downsampleTrackingService.initJob(partition));
  }

  private static boolean isNoNodeAvailable(Throwable throwable) {
    if (throwable instanceof CassandraConnectionFailureException) {
      return throwable.getCause() instanceof NoNodeAvailableException;
    }
    return false;
  }

  private Publisher<?> processDownsampleSet(PendingDownsampleSet pendingDownsampleSet, int partition) {
    Duration downsamplingDelay = Duration.between(pendingDownsampleSet.getTimeSlot(), Instant.now());
    this.meterTimer.record(downsamplingDelay.getSeconds(), TimeUnit.SECONDS);

//    log.info("Processing downsample set: {} partition: {}", pendingDownsampleSet, partition);

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
            .name("downsample.set")
            .metrics()
            .then(
                downsampleTrackingService.complete(pendingDownsampleSet, partition)
            )
            .doOnSuccess(o ->
                    log.info("Completed downsampling of set: {} timeslot: {} partition: {}",
                            pendingDownsampleSet.getSeriesSetHash(),
                            pendingDownsampleSet.getTimeSlot().getEpochSecond(), partition))
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
