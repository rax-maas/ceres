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
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.rackspace.ceres.app.utils.DateTimeUtils.epochToLocalDateTime;
import static com.rackspace.ceres.app.utils.DateTimeUtils.getLowerGranularity;

@Service
@Slf4j
@Profile("downsample")
public class DownsampleProcessor {
  private final DownsampleProperties properties;
  private final QueryService queryService;
  private final DataWriteService dataWriteService;
  private final Timer meterTimer;

  @Autowired
  public DownsampleProcessor(DownsampleProperties properties,
                             QueryService queryService,
                             DataWriteService dataWriteService,
                             MeterRegistry meterRegistry) {
    this.properties = properties;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
    this.meterTimer = meterRegistry.timer("downsampling.delay");
  }

  @PostConstruct
  public void checkGranularities() {
    if (properties.getGranularities() == null || properties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Granularities are not configured!");
    }
  }

  public Publisher<?> processDownsampleSet(PendingDownsampleSet pendingSet, int partition, String group) {
    log.trace("processDownsampleSet {} {} {}", pendingSet, partition, group);
    Duration downsamplingDelay = Duration.between(pendingSet.getTimeSlot(), Instant.now());
    this.meterTimer.record(downsamplingDelay.getSeconds(), TimeUnit.SECONDS);

    List<Granularity> granularities = DateTimeUtils.filterGroupGranularities(group, properties.getGranularities());
    final Flux<ValueSet> data = fetchData(pendingSet, group, granularities.get(0));

    return downsampleData(data, pendingSet.getTenant(), pendingSet.getSeriesSetHash(), granularities.iterator())
        .name("downsample.set")
        .tag("partition", String.valueOf(partition))
        .tag("group", group)
        .metrics()
        .doOnSuccess(o ->
            log.trace("Completed downsampling of set: {} timeslot: {} time: {} partition: {} group: {}",
                pendingSet.getSeriesSetHash(),
                pendingSet.getTimeSlot().getEpochSecond(),
                epochToLocalDateTime(pendingSet.getTimeSlot().getEpochSecond()),
                partition, group))
        .checkpoint();
  }

  public Mono<?> downsampleData(Flux<? extends ValueSet> data,
                                String tenant,
                                String seriesSet,
                                Iterator<Granularity> granularities) {
    if (!granularities.hasNext()) {
      return Mono.empty();
    }
    final Granularity granularity = granularities.next();
    final TemporalNormalizer normalizer = new TemporalNormalizer(granularity.getWidth());

    final Flux<AggregatedValueSet> aggregated = data
        .doOnNext(valueSet -> log.trace("Aggregating {} into granularity={}", valueSet, granularity))
        // group the incoming data by granularity-time-window
        .windowUntilChanged(
            valueSet -> valueSet.getTimestamp().with(normalizer), Instant::equals)
        // ...and then do the aggregation math on those
        .concatMap(valueSetFlux ->
            valueSetFlux.collect(ValueSetCollectors.gaugeCollector(granularity.getWidth())));

    return dataWriteService.storeDownsampledData(aggregated, tenant, seriesSet)
        .then(downsampleData(aggregated, tenant, seriesSet, granularities))
        .checkpoint();
  }

  private Flux<ValueSet> fetchData(PendingDownsampleSet set, String group, Granularity granularity) {
    Duration lowerWidth = getLowerGranularity(properties.getGranularities(), granularity.getWidth());
    if (isLowerGranularityRaw(lowerWidth)) {
      return queryService.queryRawWithSeriesSet(
          set.getTenant(),
          set.getSeriesSetHash(),
          set.getTimeSlot(),
          set.getTimeSlot().plus(Duration.parse(group))
      );
    } else {
      log.trace("Fetching downsampled data as base {} {} {}", set, group, lowerWidth);
      return queryService.queryDownsampled(
          set.getTenant(),
          set.getSeriesSetHash(),
          set.getTimeSlot().minus(Duration.parse(group)), // Redo the old timeslot for race conditions
          set.getTimeSlot().plus(Duration.parse(group)),
          lowerWidth);
    }
  }

  private boolean isLowerGranularityRaw(Duration lowerWidth) {
    return lowerWidth.toString().equals("PT0S");
  }
}
