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
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.model.Metadata;
import com.rackspace.ceres.app.model.QueryData;
import com.rackspace.ceres.app.model.QueryResult;
import com.rackspace.ceres.app.model.TsdbQueryRequest;
import com.rackspace.ceres.app.model.TsdbQueryResult;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class QueryService {
  private final ReactiveCqlTemplate cqlTemplate;
  private final MetadataService metadataService;
  private final DataTablesStatements dataTablesStatements;
  private final TimeSlotPartitioner timeSlotPartitioner;
  private final AppProperties appProperties;
  private final Counter dbOperationErrorsCounter;

  @Autowired
  public QueryService(ReactiveCqlTemplate cqlTemplate,
                      MetadataService metadataService,
                      DataTablesStatements dataTablesStatements,
                      TimeSlotPartitioner timeSlotPartitioner,
                      AppProperties appProperties, MeterRegistry meterRegistry) {
    this.cqlTemplate = cqlTemplate;
    this.metadataService = metadataService;
    this.dataTablesStatements = dataTablesStatements;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.appProperties = appProperties;
    dbOperationErrorsCounter = meterRegistry.counter("ceres.db.operation.errors",
        "type", "read");
  }

  public Flux<QueryResult> queryRaw(String tenant, String metricName, String metricGroup,
                                    Map<String, String> queryTags,
                                    Instant start, Instant end) {
    return !StringUtils.isBlank(metricName) ?
        getQueryResultFlux(tenant, queryTags, start, end, metricName).checkpoint() :
        metadataService.getMetricNamesFromMetricGroup(tenant, metricGroup)
            .flatMap(metric -> getQueryResultFlux(tenant, queryTags, start, end, metric))
            .checkpoint();
  }

  private Flux<QueryResult> getQueryResultFlux(String tenant, Map<String, String> queryTags,
                                               Instant start, Instant end, String metricName) {
    return metadataService.locateSeriesSetHashes(tenant, metricName, queryTags)
        // then perform a retrieval for each series-set
        .flatMap(seriesSet -> mapSeriesSetResult(tenant, seriesSet, Aggregator.raw,
            // over each time slot partition of the [start,end] range
            Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
                .concatMap(timeSlot -> cqlTemplate.queryForRows(
                            dataTablesStatements.rawQuery(),
                            tenant, timeSlot, seriesSet, start, end
                        )
                        .name("queryRaw")
                        .metrics()
                ), buildMetaData(Aggregator.raw, start, end, null)
        ));
  }

  public Flux<ValueSet> queryRawWithSeriesSet(String tenant, String seriesSet,
                                              Instant start, Instant end) {
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .concatMap(timeSlot ->
            cqlTemplate.queryForRows(dataTablesStatements.rawQuery(), tenant, timeSlot, seriesSet, start, end)
                .name("queryRawWithSeriesSet")
                .metrics()
                .retryWhen(appProperties.getRetryQueryForDownsample().build())
                .map(row -> new SingleValueSet().setValue(row.getDouble(1)).setTimestamp(row.getInstant(0)))
                .doOnError(e -> dbOperationErrorsCounter.increment())
        )
        .checkpoint();
  }

  public Flux<QueryResult> queryDownsampled(String tenant, String metricName, String metricGroup,
                                            Aggregator aggregator, Duration granularity,
                                            Map<String, String> queryTags, Instant start,
                                            Instant end) {
    return !StringUtils.isBlank(metricName) ?
        getQueryDownsampled(tenant, metricName, aggregator, granularity, queryTags, start, end).checkpoint() :
        metadataService.getMetricNamesFromMetricGroup(tenant, metricGroup).flatMap(metric ->
                getQueryDownsampled(tenant, metric, aggregator, granularity, queryTags, start, end)
            )
            .checkpoint();
  }

  private Flux<QueryResult> getQueryDownsampled(String tenant, String metricName,
                                                Aggregator aggregator, Duration granularity,
                                                Map<String, String> queryTags, Instant start,
                                                Instant end) {
    // given the queryTags filter, locate the series-set that apply
    return metadataService.locateSeriesSetHashes(tenant, metricName, queryTags)
        // then perform a retrieval for each series-set
        .flatMap(seriesSet -> mapSeriesSetResult(tenant, seriesSet, aggregator,
            // over each time slot partition of the [start,end) range
            Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, granularity))
                .concatMap(timeSlot ->
                    cqlTemplate.queryForRows(
                            dataTablesStatements.downsampleQuery(granularity),
                            tenant, timeSlot, seriesSet, start, end
                        )
                        .doOnError(e ->
                            dbOperationErrorsCounter.increment())
                        .name("queryDownsampled")
                        .metrics()
                ), buildMetaData(aggregator, start, end, granularity)
        ));
  }

  public Flux<TsdbQueryResult> queryTsdb(String tenant, List<TsdbQueryRequest> queries,
                                         Instant start, Instant end, List<Granularity> granularities) {
    return metadataService.getTsdbQueries(queries, granularities)
        .flatMap(queryWithMetaData -> metadataService.locateSeriesSetHashesFromQuery(tenant, queryWithMetaData))
        .flatMap(query -> mapTsdbSeriesSetResult(query.getMetricName(), query.getAggregator(), query.getTags(),
            timeSlotPartitioner.partitionsOverRangeFromQuery(start, end, query.getGranularity())
                .concatMap(timeSlot ->
                    queryForRows(tenant, start, end,
                        query.getGranularity(),
                        query.getAggregator(),
                        query.getSeriesSet(), timeSlot))
        ))
        .checkpoint();
  }

  private Flux<Row> queryForRows(
      String tenant, Instant start, Instant end, Duration granularity,
      Aggregator aggregator, String seriesSet, Instant timeSlot) {
    return (aggregator == Aggregator.raw) ?
        cqlTemplate.queryForRows(
                dataTablesStatements.rawQuery(), tenant, timeSlot, seriesSet, start, end
            )
            .doOnError(e -> dbOperationErrorsCounter.increment())
            .name("queryRawWithSeriesSet").metrics() :
        cqlTemplate.queryForRows(
                dataTablesStatements.downsampleQuery(granularity), tenant, timeSlot, seriesSet, start, end
            )
            .doOnError(e -> dbOperationErrorsCounter.increment())
            .name("queryTsdb").metrics();
  }

  private Mono<TsdbQueryResult> mapTsdbSeriesSetResult(
      String metricName, Aggregator aggregator, Map<String, String> tags, Flux<Row> rows) {
    return rows.map(row -> Map.entry(
                requireNonNull((Long.toString(requireNonNull(row.getInstant(0)).getEpochSecond()))),
                row.getDouble(getRowPosition(aggregator))
            )
        )
        .collectMap(Entry::getKey, Entry::getValue, LinkedHashMap::new).filter(values -> !values.isEmpty())
        .flatMap(values -> buildTsdbQueryResult(metricName, tags, values));
  }

  private Mono<TsdbQueryResult> buildTsdbQueryResult(
      String metricName, Map<String, String> tags, Map<String, Double> values) {
    return Mono.just(new TsdbQueryResult()
        .setMetric(metricName)
        .setTags(tags)
        .setAggregatedTags(Collections.emptyList()) // TODO: in case of multiple queries set this!
        .setDps(values));
  }

  private int getRowPosition(Aggregator aggregator) {
    return switch (aggregator) {
      case min, raw -> 1;
      case max -> 2;
      case sum -> 3;
      case avg -> 4;
    };
  }

  private Mono<QueryResult> mapSeriesSetResult(
      String tenant, String seriesSet, Aggregator aggregator, Flux<Row> rows, Metadata metadata) {
    return rows
        .map(row -> Map.entry(
                requireNonNull(row.getInstant(0)),
                row.getDouble(getRowPosition(aggregator))
            )
        )
        // collect the ts->value entries into an ordered, LinkedHashMap
        .collectMap(Entry::getKey, Entry::getValue, LinkedHashMap::new)
        .filter(values -> !values.isEmpty())
        .flatMap(values -> buildQueryResult(tenant, seriesSet, values, metadata));
  }

  private Mono<QueryResult> buildQueryResult(String tenant, String seriesSet,
                                             Map<Instant, Double> values, Metadata metadata) {
    return metadataService.resolveSeriesSetHash(tenant, seriesSet)
        .map(metricNameAndTags -> new QueryResult()
            .setData(buildQueryData(tenant, metricNameAndTags.getMetricName(),
                metricNameAndTags.getTags(), values))
            .setMetadata(metadata)
        );
  }

  private QueryData buildQueryData(String tenant, String metricName, Map<String, String> tags,
                                   Map<Instant, Double> values) {
    return new QueryData()
        .setTenant(tenant)
        .setMetricName(metricName)
        .setTags(tags)
        .setValues(values);
  }

  private Metadata buildMetaData(Aggregator aggregator, Instant startTime, Instant endTime,
                                 Duration granularity) {
    return new Metadata()
        .setAggregator(aggregator)
        .setStartTime(startTime)
        .setEndTime(endTime)
        .setGranularity(granularity);
  }
}
