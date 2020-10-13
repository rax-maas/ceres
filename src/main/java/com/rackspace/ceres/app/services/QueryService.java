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
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.model.MetricNameAndTags;
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
  private final SeriesSetService seriesSetService;
  private final DataTablesStatements dataTablesStatements;

  @Autowired
  public QueryService(ReactiveCqlTemplate cqlTemplate,
                      MetadataService metadataService,
                      SeriesSetService seriesSetService,
                      DataTablesStatements dataTablesStatements) {
    this.cqlTemplate = cqlTemplate;
    this.metadataService = metadataService;
    this.seriesSetService = seriesSetService;
    this.dataTablesStatements = dataTablesStatements;
  }

  public Flux<QueryResult> queryRaw(String tenant, String metricName,
                                    Map<String, String> queryTags,
                                    Instant start, Instant end) {
    return metadataService.locateSeriesSets(tenant, metricName, queryTags)
        .name("queryRaw")
        .metrics()
        .flatMapMany(Flux::fromIterable)
        .flatMap(seriesSet ->
            mapSeriesSetResult(tenant, seriesSet,
                // TODO use repository and projections
                cqlTemplate.queryForRows(
                    dataTablesStatements.queryRaw(),
                    tenant, seriesSet, start, end
                )
            )
        );
  }

  public Flux<ValueSet> queryRawWithSeriesSet(String tenant, String seriesSet,
                                              Instant start, Instant end) {
    return cqlTemplate.queryForRows(
        dataTablesStatements.queryRaw(),
        tenant, seriesSet, start, end
    )
        .map(row ->
            new SingleValueSet().setValue(row.getDouble(1)).setTimestamp(row.getInstant(0))
        );
  }

  public Flux<QueryResult> queryDownsampled(String tenant, String metricName, Aggregator aggregator,
                                            Duration granularity, Map<String, String> queryTags,
                                            Instant start, Instant end) {
    return metadataService.locateSeriesSets(tenant, metricName, queryTags)
        .name("queryDownsampled")
        .metrics()
        .flatMapMany(Flux::fromIterable)
        .flatMap(seriesSet ->
            mapSeriesSetResult(tenant, seriesSet,
                cqlTemplate.queryForRows(
                    dataTablesStatements.queryDownsampled(granularity),
                    tenant, seriesSet, aggregator.name(), start, end
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
        .map(values -> buildQueryResult(tenant, seriesSet, values));
  }

  private QueryResult buildQueryResult(String tenant, String seriesSet,
                                       Map<Instant, Double> values) {
    final MetricNameAndTags metricNameAndTags = seriesSetService.expandSeriesSet(seriesSet);

    return new QueryResult()
        .setTenant(tenant)
        .setMetricName(metricNameAndTags.getMetricName())
        .setTags(metricNameAndTags.getTags())
        .setValues(values);
  }
}
