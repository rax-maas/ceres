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

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.Row;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.model.QueryResult;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class QueryService {

  private final ReactiveCqlTemplate cqlTemplate;
  private final MetadataService metadataService;
  private final DataTablesStatements dataTablesStatements;
  private final TimeSlotPartitioner timeSlotPartitioner;
  private final AppProperties appProperties;

  @Autowired
  public QueryService(ReactiveCqlTemplate cqlTemplate,
                      MetadataService metadataService,
                      DataTablesStatements dataTablesStatements,
                      TimeSlotPartitioner timeSlotPartitioner,
                      AppProperties appProperties) {
    this.cqlTemplate = cqlTemplate;
    this.metadataService = metadataService;
    this.dataTablesStatements = dataTablesStatements;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.appProperties = appProperties;
  }

  public Flux<QueryResult> queryRaw(String tenant, String metricName,
                                    Map<String, String> queryTags,
                                    Instant start, Instant end) {
    // given the queryTags filter, locate the series-set that apply
    return metadataService.locateSeriesSetHashes(tenant, metricName, queryTags)
        // then perform a retrieval for each series-set
        .flatMap(seriesSet ->
            mapSeriesSetResult(tenant, seriesSet,
                // over each time slot partition of the [start,end) range
                Flux.fromIterable(timeSlotPartitioner
                    .partitionsOverRange(start, end, null)
                )
                    .concatMap(timeSlot ->
                        cqlTemplate.queryForRows(
                            dataTablesStatements.rawQuery(),
                            tenant, timeSlot, seriesSet, start, end
                        )
                            .name("queryRaw")
                            .metrics()
                    )
            )
        )
        .checkpoint();
  }

  public Flux<ValueSet> queryRawWithSeriesSet(String tenant, String seriesSet,
                                              Instant start, Instant end) {
    return Flux.fromIterable(timeSlotPartitioner
        .partitionsOverRange(start, end, null)
    )
        .concatMap(timeSlot ->
            cqlTemplate.queryForRows(
                dataTablesStatements.rawQuery(),
                tenant, timeSlot, seriesSet, start, end
            )
                .name("queryRawWithSeriesSet")
                .metrics()
                .retryWhen(appProperties.getRetryQueryForDownsample().build())
                .map(row ->
                    new SingleValueSet().setValue(row.getDouble(1)).setTimestamp(row.getInstant(0))
                )
        )
        .checkpoint();
  }

  public Flux<QueryResult> queryDownsampled(String tenant, String metricName, Aggregator aggregator,
                                            Duration granularity, Map<String, String> queryTags,
                                            Instant start, Instant end) {
    // given the queryTags filter, locate the series-set that apply
    return metadataService.locateSeriesSetHashes(tenant, metricName, queryTags)
        // then perform a retrieval for each series-set
        .flatMap(seriesSet ->
            mapSeriesSetResult(tenant, seriesSet,
                // over each time slot partition of the [start,end) range
                Flux.fromIterable(timeSlotPartitioner
                    .partitionsOverRange(start, end, granularity)
                )
                    .concatMap(timeSlot ->
                        cqlTemplate.queryForRows(
                            dataTablesStatements.downsampleQuery(granularity),
                            tenant, timeSlot, seriesSet, aggregator.name(), start, end
                        )
                            .name("queryDownsampled")
                            .metrics()
                    )
            )
        )
        .checkpoint();
  }

  private Mono<QueryResult> mapSeriesSetResult(String tenant, String seriesSet, Flux<Row> rows) {
    return rows
        .map(row -> Map.entry(
            requireNonNull(row.getInstant(0)),
            row.getDouble(1)
            )
        )
        // collect the ts->value entries into an ordered, LinkedHashMap
        .collectMap(Entry::getKey, Entry::getValue, LinkedHashMap::new)
        .filter(values -> !values.isEmpty())
        .flatMap(values -> buildQueryResult(tenant, seriesSet, values));
  }

  private Mono<QueryResult> buildQueryResult(String tenant, String seriesSet,
                                             Map<Instant, Double> values) {
    return metadataService.resolveSeriesSetHash(tenant, seriesSet)
        .map(metricNameAndTags ->
            new QueryResult()
                .setTenant(tenant)
                .setMetricName(metricNameAndTags.getMetricName())
                .setTags(metricNameAndTags.getTags())
                .setValues(values)
        );
  }
}
