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
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.rackspace.ceres.app.services.DelayedTrackingService.buildDelayedPending;
import static com.rackspace.ceres.app.utils.DateTimeUtils.getPartitionWidths;
import static com.rackspace.ceres.app.utils.DateTimeUtils.randomDelay;

@Service
@Slf4j
@Profile("downsample")
public class DelayedDownsampleJobProcessor {
  private final DownsampleProperties properties;
  private final DelayedTrackingService delayedTrackingService;
  private final ScheduledExecutorService executor;
  private final DownsampleProcessor downsampleProcessor;

  @Autowired
  public DelayedDownsampleJobProcessor(DownsampleProperties properties,
                                       DelayedTrackingService delayedTrackingService,
                                       ScheduledExecutorService executor,
                                       DownsampleProcessor downsampleProcessor) {
    this.properties = properties;
    this.delayedTrackingService = delayedTrackingService;
    this.downsampleProcessor = downsampleProcessor;
    this.executor = executor;
  }

  @PostConstruct
  public void setupSchedulers() {
    long initialDelay = properties.getInitialProcessingDelay().getSeconds();
    executor.schedule(this::initializeRedisJobs, 1, TimeUnit.SECONDS);
    executor.schedule(this::initializeDelayedTimeslotJobs, initialDelay, TimeUnit.SECONDS);
  }

  @PreDestroy
  public void stop() {
    executor.shutdown();
  }

  private void initializeRedisJobs() {
    IntStream.rangeClosed(0, properties.getPartitions() - 1)
        .forEach((partition) -> delayedTrackingService.freeJob(partition).subscribe());
  }

  private void initializeDelayedTimeslotJobs() {
    log.info("Start delayed downsampling jobs");
    log.info("max-downsample-delayed-job-duration: {}", properties.getMaxDownsampleDelayedJobDuration().getSeconds());
    log.info("downsample-delayed-spread-period: {}", properties.getDownsampleDelayedSpreadPeriod().getSeconds());
    IntStream.rangeClosed(0, properties.getPartitions() - 1)
        .forEach((partition) -> executor.schedule(
            () -> processDelayedTimeslotJob(partition),
            randomDelay(properties.getDownsampleDelayedSpreadPeriod().getSeconds()), TimeUnit.SECONDS));
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

  private Mono<?> processDelayedTimeSlot(int partition) {
    List<String> groups = getPartitionWidths(properties.getGranularities());
    Flux<String> partitionHashes = delayedTrackingService.getDelayedHashes(partition);
    return Flux.fromIterable(groups)
        .concatMap(group -> delayedTrackingService.getDelayedTimeSlots(partition, group)
            .concatMap(ts -> {
              long timeslot = Long.parseLong(ts.split("\\|")[0]);
              log.trace("Got delayed timeslot: {} {}", partition, timeslot);
              return partitionHashes
                  .name("processDelayedTimeSlot")
                  .tag("partition", String.valueOf(partition))
                  .tag("group", group)
                  .metrics()
                  .concatMap(hash ->
                      this.downsampleProcessor.processSet(
                          buildDelayedPending(hash, timeslot), partition, group, "delayed.set")
                  )
                  .doOnError(Throwable::printStackTrace)
                  .then(this.delayedTrackingService.deleteDelayedTimeslot(partition, group, timeslot));
            })
        )
        .then(this.delayedTrackingService.deleteDelayedHashes(partition));
  }
}
