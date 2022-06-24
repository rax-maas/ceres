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

import com.rackspace.ceres.app.config.DownsampleCacheConfig;
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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static com.rackspace.ceres.app.utils.DateTimeUtils.epochToLocalDateTime;

@Service
@Slf4j
@Profile("downsample")
public class DownsampleProcessor {
  private final DownsampleProperties properties;
  private final QueryService queryService;
  private final DataWriteService dataWriteService;
  private final Timer meterTimer;
  private final DownsampleCacheConfig downsampleCacheConfig;

  @Autowired
  public DownsampleProcessor(DownsampleProperties properties,
                             QueryService queryService,
                             DataWriteService dataWriteService,
                             MeterRegistry meterRegistry,
                             DownsampleCacheConfig downsampleCacheConfig) {
    this.properties = properties;
    this.queryService = queryService;
    this.dataWriteService = dataWriteService;
    this.meterTimer = meterRegistry.timer("downsampling.delay");
    this.downsampleCacheConfig = downsampleCacheConfig;
  }

  public Publisher<?> processDownsampleSet(PendingDownsampleSet pendingSet, int partition, String group) {
    log.trace("processDownsampleSet {} {} {}", pendingSet, partition, group);
    Duration downsamplingDelay = Duration.between(pendingSet.getTimeSlot(), Instant.now());
    this.meterTimer.record(downsamplingDelay.getSeconds(), TimeUnit.SECONDS);

    final Flux<ValueSet> data = queryService.queryRawWithSeriesSet(
        pendingSet.getTenant(),
        pendingSet.getSeriesSetHash(),
        pendingSet.getTimeSlot(),
        pendingSet.getTimeSlot().plus(Duration.parse(group))
    );

    this.downsampleCacheConfig.dataPoints().put("PT0S", data);

    return Flux.fromIterable(DateTimeUtils.filterGroupGranularities(group, properties.getGranularities()))
        .flatMap(granularity ->
            downsampleData(data,
                pendingSet.getTenant(),
                pendingSet.getSeriesSetHash(),
                granularity
            )
            .name("downsample.set")
            .tag("partition", String.valueOf(partition))
                .tag("group", group)
                .metrics()
                .doOnSuccess(o -> {
                      log.info("Completed downsampling of set: {} timeslot: {} time: {} partition: {} group: {}",
                          pendingSet.getSeriesSetHash(),
                          pendingSet.getTimeSlot().getEpochSecond(),
                          epochToLocalDateTime(pendingSet.getTimeSlot().getEpochSecond()),
                          partition, group);
                    }
                ),
            1
        );
  }

  public Mono<?> downsampleData(Flux<? extends ValueSet> data,
                                String tenant,
                                String seriesSet,
                                Granularity granularity) {
    log.info("downsampleData {} {} {}", tenant, seriesSet, granularity);

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

    this.downsampleCacheConfig.dataPoints().put(granularity.getWidth().toString(), aggregated);
    this.downsampleCacheConfig.dataPoints().remove("PT0S");

    // expand the aggregated volume-sets into individual data points to be stored
    final Flux<DataDownsampled> expanded = expandAggregatedData(aggregated, tenant, seriesSet);

    return dataWriteService.storeDownsampledData(expanded).checkpoint();
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
