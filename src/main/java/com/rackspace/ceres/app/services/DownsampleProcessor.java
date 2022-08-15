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

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static com.rackspace.ceres.app.utils.DateTimeUtils.epochToLocalDateTime;

@Service
@Slf4j
@Profile("downsample")
public class DownsampleProcessor {
  private final DownsampleProperties properties;
  private final DownsamplingService downsamplingService;
  private final QueryService queryService;
  private final Timer meterTimer;

  @Autowired
  public DownsampleProcessor(DownsampleProperties properties,
                             DownsamplingService downsamplingService,
                             QueryService queryService,
                             MeterRegistry meterRegistry) {
    this.properties = properties;
    this.downsamplingService = downsamplingService;
    this.queryService = queryService;
    this.meterTimer = meterRegistry.timer("downsampling.delay");
  }

  @PostConstruct
  public void checkGranularities() {
    if (properties.getGranularities() == null || properties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Granularities are not configured!");
    }
  }

  public Publisher<?> processSet(PendingDownsampleSet pendingSet, int partition, String group, String metrics) {
    Duration downsamplingDelay = Duration.between(pendingSet.getTimeSlot(), Instant.now());
    this.meterTimer.record(downsamplingDelay.getSeconds(), TimeUnit.SECONDS);

    return Flux.fromIterable(DateTimeUtils.filterGroupGranularities(group, properties.getGranularities()))
        .concatMap(granularity ->
            this.downsamplingService.downsampleData(pendingSet, granularity.getWidth(),
                this.queryService.fetchData(pendingSet, group, granularity.getWidth(), true))
        )
        .name(metrics)
        .tag("partition", String.valueOf(partition))
        .tag("group", group)
        .metrics()
        .doOnComplete(() ->
            log.trace("Completed downsampling of set: {} timeslot: {} time: {} partition: {} group: {}",
                pendingSet.getSeriesSetHash(),
                pendingSet.getTimeSlot().getEpochSecond(),
                epochToLocalDateTime(pendingSet.getTimeSlot().getEpochSecond()),
                partition, group))
        .checkpoint();
  }
}
