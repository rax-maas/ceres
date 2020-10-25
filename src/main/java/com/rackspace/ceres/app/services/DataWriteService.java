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

import static com.rackspace.ceres.app.services.DataTablesStatements.INSERT_RAW;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.downsample.DataDownsampled;
import com.rackspace.ceres.app.model.Metric;
import java.time.Duration;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Service
@Slf4j
public class DataWriteService {

  private final ReactiveCqlTemplate cqlTemplate;
  private final SeriesSetService seriesSetService;
  private final MetadataService metadataService;
  private final DataTablesStatements dataTablesStatements;
  private final DownsampleTrackingService downsampleTrackingService;
  private final AppProperties appProperties;

  @Autowired
  public DataWriteService(ReactiveCqlTemplate cqlTemplate,
                          SeriesSetService seriesSetService,
                          MetadataService metadataService,
                          DataTablesStatements dataTablesStatements,
                          DownsampleTrackingService downsampleTrackingService,
                          AppProperties appProperties) {
    this.cqlTemplate = cqlTemplate;
    this.seriesSetService = seriesSetService;
    this.metadataService = metadataService;
    this.dataTablesStatements = dataTablesStatements;
    this.downsampleTrackingService = downsampleTrackingService;
    this.appProperties = appProperties;
  }

  public Flux<Metric> ingest(Flux<Tuple2<String,Metric>> metrics) {
    return metrics.flatMap(tuple -> ingest(tuple.getT1(), tuple.getT2()));
  }

  public Mono<Metric> ingest(String tenant, Metric metric) {
    cleanTags(metric.getTags());

    final String seriesSetHash = seriesSetService
        .hash(metric.getMetric(), metric.getTags());

    log.trace("Ingesting metric={} for tenant={}", metric, tenant);

    return
        storeRawData(tenant, metric, seriesSetHash)
            .name("ingest")
            .metrics()
            .and(metadataService.storeMetadata(tenant, seriesSetHash, metric.getMetric(),
                metric.getTags()))
            .and(downsampleTrackingService.track(tenant, seriesSetHash, metric.getTimestamp()))
            .then(Mono.just(metric));
  }

  private void cleanTags(Map<String, String> tags) {
    tags.entrySet()
        .removeIf(entry ->
            !StringUtils.hasText(entry.getKey()) ||
            !StringUtils.hasText(entry.getValue()));
  }

  private Mono<?> storeRawData(String tenant, Metric metric, String seriesSetHash) {
    return cqlTemplate.execute(
        INSERT_RAW,
        tenant, seriesSetHash, metric.getTimestamp(), metric.getValue().doubleValue()
    )
        .retryWhen(appProperties.getRetryInsertRaw().build())
        .checkpoint();
  }

  public Flux<Tuple2<DataDownsampled,Boolean>> storeDownsampledData(Flux<DataDownsampled> data, Duration ttl) {
    return data.flatMap(entry ->
        cqlTemplate.execute(
            dataTablesStatements.downsampleInsert(entry.getGranularity()),
            entry.getTenant(), entry.getSeriesSetHash(), entry.getAggregator().name(),
            entry.getTs(), entry.getValue()
        )
            .retryWhen(appProperties.getRetryInsertDownsampled().build())
            .map(applied -> Tuples.of(entry, applied))
    )
        .checkpoint();
  }
}
