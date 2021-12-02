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

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.downsample.DataDownsampled;
import com.rackspace.ceres.app.model.Metric;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class DataWriteService {

  private static final String LABEL_METRIC_GROUP = "metricGroup";
  private static final String LABEL_RESOURCE = "resource";
  private final ReactiveCqlTemplate cqlTemplate;
  private final SeriesSetService seriesSetService;
  private final MetadataService metadataService;
  private final DataTablesStatements dataTablesStatements;
  private final TimeSlotPartitioner timeSlotPartitioner;
  private final DownsampleTrackingService downsampleTrackingService;
  private final AppProperties appProperties;
  private final Timer latencyTimer;

  @Autowired
  public DataWriteService(ReactiveCqlTemplate cqlTemplate,
                          SeriesSetService seriesSetService,
                          MetadataService metadataService,
                          DataTablesStatements dataTablesStatements,
                          TimeSlotPartitioner timeSlotPartitioner,
                          DownsampleTrackingService downsampleTrackingService,
                          AppProperties appProperties,
                          MeterRegistry meterRegistry) {
    this.cqlTemplate = cqlTemplate;
    this.seriesSetService = seriesSetService;
    this.metadataService = metadataService;
    this.dataTablesStatements = dataTablesStatements;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.downsampleTrackingService = downsampleTrackingService;
    this.appProperties = appProperties;
    this.latencyTimer = meterRegistry.timer("ingest.latency");
  }

  public Flux<Metric> ingest(Flux<Tuple2<String, Metric>> metrics) {
    return metrics.flatMap(tuple -> ingest(tuple.getT1(), tuple.getT2()));
  }

  public Mono<Metric> ingest(String tenant, Metric metric) {
    log.trace("Ingesting metric={} for tenant={}", metric, tenant);

    String metricGroupTag = metric.getTags().get(LABEL_METRIC_GROUP);
    String resourceTag = metric.getTags().get(LABEL_RESOURCE);
    if (!StringUtils.hasText(metricGroupTag) || !StringUtils.hasText(resourceTag)) {
      return Mono.error(new ServerWebInputException("metricGroup tag and resource tag must be present"));
    }

    cleanTags(metric.getTags());

    final String seriesSetHash = seriesSetService.hash(metric.getMetric(), metric.getTags());

    return storeRawData(tenant, metric, seriesSetHash)
            .name("ingest")
            .metrics()
            .and(metadataService.storeMetadata(tenant, seriesSetHash, metric.getMetric(), metric.getTags()))
            .and(downsampleTrackingService.track(tenant, seriesSetHash, metric.getTimestamp()))
            .and(storeMetricGroup(tenant, metric))
            .and(storeDeviceData(tenant, metric))
            .then(Mono.just(metric))
            .doOnNext(_metric -> recordIngestionLatency(_metric));
  }

  private void recordIngestionLatency(Metric metric) {
    latencyTimer.record(Duration.between(metric.getTimestamp(), Instant.now()).getSeconds(), TimeUnit.SECONDS);
  }

  private void cleanTags(Map<String, String> tags) {
    tags.entrySet()
        .removeIf(entry ->
            !StringUtils.hasText(entry.getKey()) ||
                !StringUtils.hasText(entry.getValue()));
  }

  private Mono<?> storeRawData(String tenant, Metric metric, String seriesSetHash) {
    return cqlTemplate.execute(
        dataTablesStatements.rawInsert(),
        tenant,
        timeSlotPartitioner.rawTimeSlot(metric.getTimestamp()),
        seriesSetHash,
        metric.getTimestamp(),
        metric.getValue().doubleValue()
    )
        .retryWhen(appProperties.getRetryInsertRaw().build())
        .checkpoint();
  }

  /**
   * Stores a batch of downsampled data where it is assumed the flux contains data
   * of the same tenant, series-set, and granularity.
   *
   * @param data flux of data to be stored in a downsampled data table
   * @return a mono that completes when the batch is stored
   */
  public Mono<?> storeDownsampledData(Flux<DataDownsampled> data) {
    return data
        // convert each data point to an insert-statement
        .map(entry ->
            new SimpleStatementBuilder(
                dataTablesStatements.downsampleInsert(entry.getGranularity())
            )
                .addPositionalValues(
                    entry.getTenant(),
                    timeSlotPartitioner.downsampledTimeSlot(entry.getTs(), entry.getGranularity()),
                    entry.getSeriesSetHash(), entry.getAggregator().name(),
                    entry.getTs(), entry.getValue()
                )
                .build()
        )
        .collectList()
        // ...and create a batch statement containing those
        .map(statements -> {
          final BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(
              BatchType.LOGGED);
          // NOTE: tried addStatements, but unable to cast iterables
          statements.forEach(batchStatementBuilder::addStatement);
          return batchStatementBuilder.build();
        })
        // ...and execute the batch
        .flatMap(cqlTemplate::execute)
        .retryWhen(appProperties.getRetryInsertDownsampled().build())
        .checkpoint();
  }

  private Mono<?> storeMetricGroup(String tenant, Metric metric) {
    String updatedAt = metric.getTimestamp().toString();
    String metricGroup = metric.getTags().get(LABEL_METRIC_GROUP);
    String metricName = metric.getMetric();
    return metadataService.updateMetricGroupAddMetricName(tenant, metricGroup, metricName, updatedAt);
  }

  private Mono<?> storeDeviceData(String tenant, Metric metric) {
    String updatedAt = metric.getTimestamp().toString();
    String device = metric.getTags().get(LABEL_RESOURCE);
    String metricName = metric.getMetric();
    return metadataService.updateDeviceAddMetricName(tenant, device, metricName, updatedAt);
  }
}
