/*
 * Copyright 2022 Rackspace US, Inc.
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.rackspace.ceres.app.utils.DateTimeUtils.*;
import static com.rackspace.ceres.app.services.DelayedTrackingService.*;

@Service
@Slf4j
@Profile("downsample")
public class DownsampleJobProcessor {
  private final DownsampleProperties properties;
  private final DownsampleTrackingService trackingService;
  private final DelayedTrackingService delayedTrackingService;
  private final ScheduledExecutorService executor;
  private final DownsampleProcessor downsampleProcessor;

  @Autowired
  public DownsampleJobProcessor(DownsampleProperties properties,
                                DownsampleTrackingService trackingService,
                                DelayedTrackingService delayedTrackingService,
                                ScheduledExecutorService executor,
                                DownsampleProcessor downsampleProcessor) {
    this.properties = properties;
    this.trackingService = trackingService;
    this.delayedTrackingService = delayedTrackingService;
    this.downsampleProcessor = downsampleProcessor;
    this.executor = executor;
  }

  @PostConstruct
  public void setupSchedulers() {
    if (properties.getGranularities() == null ||
        properties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Granularities are not configured!");
    }
    long initialDelay = properties.getInitialProcessingDelay().getSeconds();
    executor.schedule(this::initializeRedisJobs, 1, TimeUnit.SECONDS);
    executor.schedule(this::initializeJobs, initialDelay, TimeUnit.SECONDS);
    executor.schedule(this::initializeDelayedTimeslotJobs, initialDelay, TimeUnit.SECONDS);
  }

  @PreDestroy
  public void stop() {
    executor.shutdown();
  }

  private void initializeRedisJobs() {
    getPartitionWidths(properties.getGranularities())
        .forEach(width -> IntStream.rangeClosed(0, properties.getPartitions() - 1)
            .forEach((partition) -> trackingService.freeJob(partition, width).subscribe()));
    IntStream.rangeClosed(0, properties.getPartitions() - 1)
        .forEach((partition) -> delayedTrackingService.freeJob(partition).subscribe());
  }

  private void initializeJobs() {
    log.info("Start downsampling jobs");
    log.info("Downsampling configuration parameters");
    log.info("=====================================");
    log.info("downsample-spread-period: {}", properties.getDownsampleSpreadPeriod().getSeconds());
    log.info("max-concurrent-downsample-hashes: {}", properties.getMaxConcurrentDownsampleHashes());
    log.info("max-downsample-job-duration: {}", properties.getMaxDownsampleJobDuration().getSeconds());
    log.info("max-downsample-delayed-job-duration: {}", properties.getMaxDownsampleDelayedJobDuration().getSeconds());
    log.info("downsample-delayed-spread-period: {}", properties.getDownsampleDelayedSpreadPeriod().getSeconds());
    log.info("=====================================");

    getPartitionWidths(properties.getGranularities())
        .forEach(width -> IntStream.rangeClosed(0, properties.getPartitions() - 1)
            .forEach((partition) -> executor.schedule(() ->
                    processJob(partition, width),
                randomDelay(properties.getDownsampleSpreadPeriod().getSeconds()), TimeUnit.SECONDS)));
  }

  private void initializeDelayedTimeslotJobs() {
    IntStream.rangeClosed(0, properties.getPartitions() - 1)
        .forEach((partition) -> executor.schedule(
            () -> processDelayedTimeslotJob(partition),
            randomDelay(properties.getDownsampleDelayedSpreadPeriod().getSeconds()), TimeUnit.SECONDS));
  }

  private void processJob(int partition, String group) {
    log.trace("processJob {} {}", partition, group);
    trackingService.claimJob(partition, group)
        .flatMap(status -> status.equals("free") ?
            processTimeSlot(partition, group).then(trackingService.freeJob(partition, group)) : Flux.empty()
        ).subscribe();
    executor.schedule(() -> processJob(partition, group),
        randomDelay(properties.getDownsampleSpreadPeriod().getSeconds()), TimeUnit.SECONDS);
  }

  private void processDelayedTimeslotJob(int partition) {
    log.trace("processDelayedTimeslotJob {}", partition);
    delayedTrackingService.claimJob(partition)
        .flatMap(status -> status.equals("free") ?
            processDelayedTimeSlot(partition)
                .then(delayedTrackingService.freeJob(partition)) : Flux.empty()
        ).subscribe();
    executor.schedule(() -> processDelayedTimeslotJob(partition),
        randomDelay(properties.getDownsampleDelayedSpreadPeriod().getSeconds()), TimeUnit.SECONDS);
  }

  private Flux<?> processTimeSlot(int partition, String group) {
    log.trace("processTimeSlot {} {}", partition, group);
    return trackingService.getTimeSlot(partition, group)
        .flatMapMany(ts -> {
          long timeslot = Long.parseLong(ts);
          log.info("Got timeslot: {} {} {}", partition, group, epochToLocalDateTime(timeslot));
          return trackingService.getDownsampleSets(timeslot, partition)
              .name("processTimeSlot")
              .tag("partition", String.valueOf(partition))
              .tag("group", group)
              .metrics()
              .flatMap(downsampleSet -> this.downsampleProcessor.processDownsampleSet(downsampleSet, partition, group),
                  properties.getMaxConcurrentDownsampleHashes())
              .then(trackingService.deleteTimeslot(partition, group, timeslot))
              .doOnError(Throwable::printStackTrace);
        });
  }

  private Flux<?> processDelayedTimeSlot(int partition) {
    return Flux.fromIterable(getPartitionWidths(properties.getGranularities()))
        .flatMap(group -> delayedTrackingService.getDelayedTimeSlots(partition, group)
                .map(tsString -> buildDownsampleSet(partition, group, tsString))
                .name("processDelayedTimeSlot")
                .tag("partition", String.valueOf(partition))
                .tag("group", group)
                .metrics()
                .flatMap(downsampleSet ->
                    Mono.from(this.downsampleProcessor.processDownsampleSet(downsampleSet, partition, group))
                    .then(delayedTrackingService.deleteDelayedTimeslot(
                        partition, group, encodeDelayedTimeslot(downsampleSet))),
                    properties.getMaxConcurrentDownsampleHashes())
                .doOnError(Throwable::printStackTrace),
            properties.getMaxConcurrentDownsampleHashes()
        );
  }
}
