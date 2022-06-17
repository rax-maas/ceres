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

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.*;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Service
@Slf4j
@Profile("downsample")
public class DownsampleProcessor {

  private final DownsampleProperties properties;
  private final DownsampleTrackingService trackingService;
  private final QueryService queryService;
  private final DataWriteService dataWriteService;
  private final Timer meterTimer;
  private final ScheduledExecutorService executor;
  private final MeterRegistry meterRegistry;

  @Autowired
  public DownsampleProcessor(DownsampleProperties properties,
                             DownsampleTrackingService trackingService,
                             QueryService queryService,
                             DataWriteService dataWriteService,
                             MeterRegistry meterRegistry,
                             ScheduledExecutorService executor) {
    this.properties = properties;
    this.trackingService = trackingService;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
    this.meterTimer = meterRegistry.timer("downsampling.delay");
    this.executor = executor;
    this.meterRegistry = meterRegistry;
  }

  @PostConstruct
  public void setupSchedulers() {
    if (properties.getGranularities() == null ||
        properties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Granularities are not configured!");
    }
    executor.schedule(this::initializeRedisJobs, 1, TimeUnit.SECONDS);
    executor.schedule(this::initializeJobs, properties.getInitialProcessingDelay().getSeconds(), TimeUnit.SECONDS);
  }

  @PreDestroy
  public void stop() {
    executor.shutdown();
  }

  private void initializeRedisJobs() {
    DateTimeUtils.getPartitionWidths(properties.getGranularities())
        .forEach(width -> IntStream.rangeClosed(0, properties.getPartitions() - 1)
            .forEach((partition) -> initRedisJob(partition, width)));
  }

  private int nextJobScheduleDelay() {
    int maxInterval = Long.valueOf(properties.getDownsampleSpreadPeriod().getSeconds()).intValue();
    int minSchedulingInterval = 1;
    return new Random().nextInt(maxInterval - minSchedulingInterval) + minSchedulingInterval;
  }

  private void initializeJobs() {
    log.info("Start downsampling jobs");
    log.info("Downsampling configuration parameters");
    log.info("=====================================");
    log.info("downsample-spread-period: {}", properties.getDownsampleSpreadPeriod().getSeconds());
    log.info("max-concurrent-downsample-hashes: {}", properties.getMaxConcurrentDownsampleHashes());
    log.info("max-downsample-job-duration: {}", properties.getMaxDownsampleJobDuration().getSeconds());
    log.info("=====================================");

    DateTimeUtils.getPartitionWidths(properties.getGranularities())
        .forEach(width -> IntStream.rangeClosed(0, properties.getPartitions() - 1)
            .forEach((partition) -> executor.schedule(
                () -> processJob(partition, width), nextJobScheduleDelay(), TimeUnit.SECONDS)));
  }

  private void processJob(int partition, String group) {
    log.trace("processJob {} {}", partition, group);
    trackingService.claimJob(partition, group)
            .flatMap(status -> {
              if (status.equals("free")) {
                log.trace("Job claimed: {} {}", partition, group);
                return processTimeSlot(partition, group);
              } else {
                return Flux.empty();
              }
            }).subscribe(o -> {}, Throwable::printStackTrace);
    trackingService.freeJob(partition, group).subscribe(o -> {}, throwable -> {});
    executor.schedule(() -> processJob(partition, group), nextJobScheduleDelay(), TimeUnit.SECONDS);
  }

  private void initRedisJob(int partition, String group) {
    trackingService.freeJob(partition, group).subscribe(o -> {}, throwable -> {});
  }

  private Flux<?> processTimeSlot(int partition, String group) {
    log.trace("processTimeSlot {} {}", partition, group);
    return trackingService.getTimeSlot(partition, group)
            .flatMapMany(timeslotString -> {
              long timeslot = Long.parseLong(timeslotString);
              log.info("Got timeslot: {} {} {}", partition, group, DateTimeUtils.epochToLocalDateTime(timeslot));
              return trackingService.getDownsampleSets(timeslot, partition)
                      .name("processTimeSlot")
                      .tag("partition", String.valueOf(partition))
                      .tag("group", group)
                      .metrics()
                      .flatMap(downsampleSet -> processDownsampleSet(downsampleSet, partition, group), properties.getMaxConcurrentDownsampleHashes())
                      .doOnError(Throwable::printStackTrace);
            });
  }

  private Publisher<?> processDownsampleSet(PendingDownsampleSet pendingDownsampleSet, int partition, String group) {
    log.trace("processDownsampleSet {} {} {}", pendingDownsampleSet, partition, group);
    Duration downsamplingDelay = Duration.between(pendingDownsampleSet.getTimeSlot(), Instant.now());
    this.meterTimer.record(downsamplingDelay.getSeconds(), TimeUnit.SECONDS);

    final Flux<ValueSet> data = queryService.queryRawWithSeriesSet(
        pendingDownsampleSet.getTenant(),
        pendingDownsampleSet.getSeriesSetHash(),
        pendingDownsampleSet.getTimeSlot(),
        pendingDownsampleSet.getTimeSlot().plus(Duration.parse(group))
    );

    List<Granularity> granularities = DateTimeUtils.filterGroupGranularities(group, properties.getGranularities());
    return
        downsampleData(data,
            pendingDownsampleSet.getTenant(),
            pendingDownsampleSet.getSeriesSetHash(),
            granularities.iterator()
        )
            .name("downsample.set")
            .tag("partition", String.valueOf(partition))
            .tag("group", group)
            .metrics()
            .doOnSuccess(o ->
                log.trace("Completed downsampling of set: {} timeslot: {} time: {} partition: {} group: {}",
                    pendingDownsampleSet.getSeriesSetHash(),
                    pendingDownsampleSet.getTimeSlot().getEpochSecond(),
                    DateTimeUtils.epochToLocalDateTime(pendingDownsampleSet.getTimeSlot().getEpochSecond()),
                    partition, group))
            .checkpoint();
  }

  /**
   * Downsample the given data into the next granularity, stores that data,
   * and recurses until the remaining granularities are processed.
   *
   * @param data          a flux of either raw {@link SingleValueSet}s or
   *                      aggregated {@link AggregatedValueSet}s from the prior granularity.
   * @param tenant        the tenant of the pending downsample set
   * @param seriesSet     the series-set of the pending downsample set
   * @param granularities remaining granularities to process
   * @return a mono that completes when the aggregated data has been stored
   */
  public Mono<?> downsampleData(Flux<? extends ValueSet> data,
                                String tenant,
                                String seriesSet,
                                Iterator<Granularity> granularities) {
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
            .concatMap(valueSetFlux ->
                valueSetFlux.collect(ValueSetCollectors.gaugeCollector(granularity.getWidth())));

    // expand the aggregated volume-sets into individual data points to be stored
    final Flux<DataDownsampled> expanded = expandAggregatedData(aggregated, tenant, seriesSet);

    return dataWriteService.storeDownsampledData(expanded)
        .then(
            // ...and recurse into remaining granularities
            downsampleData(aggregated, tenant, seriesSet, granularities)
        )
        .checkpoint();
  }

  public Flux<DataDownsampled> expandAggregatedData(Flux<AggregatedValueSet> aggs, String tenant, String seriesSet) {
    return aggs.flatMap(agg -> Flux.just(
        data(tenant, seriesSet, agg).setAggregator(Aggregator.sum).setValue(agg.getSum()),
        data(tenant, seriesSet, agg).setAggregator(Aggregator.min).setValue(agg.getMin()),
        data(tenant, seriesSet, agg).setAggregator(Aggregator.max).setValue(agg.getMax()),
        data(tenant, seriesSet, agg).setAggregator(Aggregator.avg).setValue(agg.getAverage())
    ));
  }

  private static DataDownsampled data(String tenant, String seriesSet, AggregatedValueSet agg) {
    return new DataDownsampled()
        .setTs(agg.getTimestamp())
        .setGranularity(agg.getGranularity())
        .setTenant(tenant)
        .setSeriesSetHash(seriesSet);
  }
}
