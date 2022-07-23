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
import com.rackspace.ceres.app.downsample.AggregatedValueSet;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.downsample.ValueSetCollectors;
import com.rackspace.ceres.app.helper.MetricDeletionHelper;
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
  private final MetricDeletionHelper metricDeletionHelper;

  @Autowired
  public DownsampleProcessor(DownsampleProperties properties,
                             QueryService queryService,
                             DataWriteService dataWriteService,
                             MeterRegistry meterRegistry,
                             MetricDeletionHelper metricDeletionHelper) {
    this.properties = properties;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
    this.meterTimer = meterRegistry.timer("downsampling.delay");
    this.metricDeletionHelper = metricDeletionHelper;
  }

  @PostConstruct
  public void checkGranularities() {
    if (properties.getGranularities() == null || properties.getGranularities().isEmpty()) {
      throw new IllegalStateException("Granularities are not configured!");
    }
  }

  public Publisher<?> processDownsampleSet(PendingDownsampleSet pendingSet, int partition, String group) {
    log.trace("processDownsampleSet {} {} {}", pendingSet, partition, group);
    return process(pendingSet, partition, group, "downsample.set");
  }

  public Publisher<?> processDelayedDownsampleSet(PendingDownsampleSet pendingSet, int partition, String group) {
    log.trace("processDelayedDownsampleSet {} {} {}", pendingSet, partition, group);
    return process(pendingSet, partition, group, "delayed.set");
  }

  private Publisher<?> process(PendingDownsampleSet pendingSet, int partition, String group, String metrics) {
    log.trace("process {} {} {}", pendingSet, partition, group);
    Duration downsamplingDelay = Duration.between(pendingSet.getTimeSlot(), Instant.now());
    this.meterTimer.record(downsamplingDelay.getSeconds(), TimeUnit.SECONDS);

    return Flux.fromIterable(DateTimeUtils.filterGroupGranularities(group, properties.getGranularities()))
        .concatMap(granularity -> metrics.equals("downsample.set") ?
            downsampleData(pendingSet, group, granularity) :
            downsampleData(pendingSet, group, granularity)
                .then(this.metricDeletionHelper.deleteDelayedHash(partition, group, encodeDelayedHash(pendingSet))))
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

  public Mono<?> downsampleData(PendingDownsampleSet pendingSet, String group, Granularity granularity) {
    final Flux<AggregatedValueSet> aggregated = fetchData(pendingSet, group, granularity)
        .windowUntilChanged(valueSet ->
            valueSet.getTimestamp().with(new TemporalNormalizer(granularity.getWidth())), Instant::equals)
        .concatMap(valueSetFlux -> valueSetFlux.collect(ValueSetCollectors.gaugeCollector(granularity.getWidth())));

    return dataWriteService.storeDownsampledData(aggregated, pendingSet.getTenant(), pendingSet.getSeriesSetHash())
        .checkpoint();
  }

  private Flux<ValueSet> fetchData(PendingDownsampleSet set, String group, Granularity granularity) {
    Duration lowerWidth = getLowerGranularity(properties.getGranularities(), granularity.getWidth());
    return isLowerGranularityRaw(lowerWidth) ?
        queryService.queryRawWithSeriesSet(
            set.getTenant(),
            set.getSeriesSetHash(),
            set.getTimeSlot(),
            set.getTimeSlot().plus(Duration.parse(group))
        )
        :
        queryService.queryDownsampled(
            set.getTenant(),
            set.getSeriesSetHash(),
            set.getTimeSlot().minus(Duration.parse(group)), // Redo the old timeslot in case of race conditions
            set.getTimeSlot().plus(Duration.parse(group)),
            lowerWidth);
  }

  private boolean isLowerGranularityRaw(Duration lowerWidth) {
    return lowerWidth.toString().equals("PT0S");
  }

  private String encodeDelayedHash(PendingDownsampleSet set) {
    return String.format("%d|%s|%s", set.getTimeSlot().getEpochSecond(), set.getTenant(), set.getSeriesSetHash());
  }
}
