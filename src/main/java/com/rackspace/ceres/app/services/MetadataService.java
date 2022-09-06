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

import static com.rackspace.ceres.app.entities.SeriesSetHash.COL_SERIES_SET_HASH;
import static com.rackspace.ceres.app.entities.SeriesSetHash.COL_TENANT;
import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.model.Criteria;
import com.rackspace.ceres.app.model.Filter;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricDTO;
import com.rackspace.ceres.app.model.MetricNameAndMultiTags;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import com.rackspace.ceres.app.model.SuggestType;
import com.rackspace.ceres.app.model.TsdbQuery;
import com.rackspace.ceres.app.model.TsdbQueryRequest;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class MetadataService {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String DELIM = "|";
  private static final String TAG_VALUE_REGEX = ".*\\{.*\\}$";
  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache;
  private final Counter cassandraHit;
  private final Counter cassandraMiss;
  private final Counter writeDbOperationErrorsCounter;
  private final Counter readDbOperationErrorsCounter;
  private final AppProperties appProperties;
  private final ElasticSearchService elasticSearchService;

  // use GROUP BY since unable to SELECT DISTINCT on primary key column
  private static final String GET_TENANT_QUERY = "SELECT tenant FROM metric_names GROUP BY tenant";
  private static final String GET_METRIC_NAMES_QUERY =
      "SELECT metric_name FROM metric_names WHERE tenant = ?";
  private static final String GET_SERIES_SET_HASHES_QUERY = "SELECT series_set_hash "
      + "FROM series_sets WHERE tenant = ? AND metric_name = ? AND tag_key = ? AND tag_value = ?";
  private static final String GET_TAG_KEYS_OR_VALUES_FROM_TAGS_DATA = "SELECT data from tags_data where tenant = ? AND type = ?";

  @Autowired
  public MetadataService(ReactiveCqlTemplate cqlTemplate,
      ReactiveCassandraTemplate cassandraTemplate,
      AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache,
      MeterRegistry meterRegistry,
      AppProperties appProperties,
      ElasticSearchService elasticSearchService) {
    this.cqlTemplate = cqlTemplate;
    this.cassandraTemplate = cassandraTemplate;
    this.seriesSetExistenceCache = seriesSetExistenceCache;

    cassandraHit = meterRegistry.counter("seriesSetHash.cassandraCache", "result", "hit");
    cassandraMiss = meterRegistry.counter("seriesSetHash.cassandraCache", "result", "miss");
    writeDbOperationErrorsCounter = meterRegistry.counter("ceres.db.operation.errors",
        "type", "write");
    readDbOperationErrorsCounter = meterRegistry.counter("ceres.db.operation.errors",
        "type", "read");
    this.appProperties = appProperties;
    this.elasticSearchService = elasticSearchService;
  }

  public Publisher<?> storeMetadata(String tenant, String seriesSetHash, Metric metric) {
    final CompletableFuture<Boolean> result = seriesSetExistenceCache.get(
        new SeriesSetCacheKey(tenant, seriesSetHash),
        (key, executor) ->
            cassandraTemplate.exists(
                    query(
                        where(COL_TENANT).is(tenant),
                        where(COL_SERIES_SET_HASH).is(seriesSetHash)
                    ),
                    SeriesSetHash.class
                )
                .name("metadataExists")
                .metrics()
                .retryWhen(appProperties.getRetryInsertMetadata().build())
                .doOnError(t -> log.error("Exception in exists check " + t.getMessage()))
                .doOnNext(exists -> {
                  if (exists) {
                    cassandraHit.increment();
                  } else {
                    cassandraMiss.increment();
                  }
                })
                // not cached in cassandra?
                .flatMap(exists -> exists ?
                    Mono.just(true) :
                    // not cached, so store the metadata to be sure
                    storeMetadataInCassandra(tenant, seriesSetHash, metric)
                        .and(elasticSearchService.saveMetricToES(tenant, metric))
                        .map(it -> true))
                .toFuture()
    );
    return Mono.fromFuture(result);
  }


  private Mono<?> storeMetadataInCassandra(String tenant, String seriesSetHash, Metric metric) {
    String metricName = metric.getMetric();
    Map<String, String> tags = metric.getTags();
    return cassandraTemplate.insert(
            new SeriesSetHash()
                .setTenant(tenant)
                .setSeriesSetHash(seriesSetHash)
                .setMetricName(metricName)
                .setTags(tags)
        )
        .retryWhen(
            appProperties.getRetryInsertMetadata().build()
        )
        .and(
            cassandraTemplate.insert(
                    new MetricName().setTenant(tenant)
                        .setMetricName(metricName)
                )
                .retryWhen(
                    appProperties.getRetryInsertMetadata().build()
                )
                .and(
                    Flux.fromIterable(tags.entrySet())
                        .flatMap(tagsEntry ->
                            Flux.concat(
                                cassandraTemplate.insert(
                                        new SeriesSet()
                                            .setTenant(tenant)
                                            .setMetricName(metricName)
                                            .setTagKey(tagsEntry.getKey())
                                            .setTagValue(tagsEntry.getValue())
                                            .setSeriesSetHash(seriesSetHash)
                                    )
                                    .retryWhen(
                                        appProperties.getRetryInsertMetadata().build()
                                    )
                            )
                        )
                )
        )
        .doOnError(e -> writeDbOperationErrorsCounter.increment());
  }

  public Mono<List<String>> getTenants() {
    return cqlTemplate.queryForFlux(GET_TENANT_QUERY,
            String.class
        )
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .collectList();
  }

  public Mono<List<String>> getMetricNames(String tenant) {
    return cqlTemplate.queryForFlux(GET_METRIC_NAMES_QUERY,
            String.class,
            tenant
        )
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .collectList();
  }

  public Flux<String> getMetricNamesFromMetricGroup(String tenant, String metricGroup) {
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.metricGroup");
    filter.setFilterValue(metricGroup);
    criteria.setIncludeFields(List.of("metricName"));
    criteria.setFilter(List.of(filter));

    try {
      List<MetricDTO> metricDTOList = elasticSearchService.search(tenant, criteria);
      List<String> metrics = metricDTOList.stream().map(e -> e.getMetricName())
          .collect(Collectors.toList());
      return Flux.fromIterable(metrics);
    } catch (IOException e) {
      log.error("error occurred while getMetricNamesUsingMetricGroup from es ", e);
      return Flux.error(e);
    }
  }

  public Flux<Map<String, String>> getTags(String tenantHeader, String metricName) {
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("metricName");
    filter.setFilterValue(metricName);
    criteria.setIncludeFields(List.of("tags"));
    criteria.setFilter(List.of(filter));

    try {
      List<MetricDTO> metricDTOList = elasticSearchService.search(tenantHeader, criteria);
      Map<String, String> tags = new HashMap<>();
      metricDTOList.stream().map(e -> e.getTags()).forEach(e -> tags.putAll(e));
      return Flux.just(tags);
    } catch (IOException e) {
      log.error("error occurred while getMetricNamesUsingMetricGroup from es ", e);
      return Flux.error(e);
    }
  }

  /**
   * Locates the recorded series-sets (<code>metricName,tagK=tagV,...</code>) that match the given
   * search criteria.
   *
   * @param tenant     series-sets are located by this tenant
   * @param metricName series-sets are located by this metric name
   * @param queryTags  series-sets are located by and'ing these tag key-value pairs
   * @return the matching series-sets
   */
  public Flux<String> locateSeriesSetHashes(String tenant, String metricName,
      Map<String, String> queryTags) {
    return Flux.fromIterable(queryTags.entrySet())
        // find the series-sets for each query tag
        .flatMap(tagEntry ->
            cqlTemplate.queryForFlux(GET_SERIES_SET_HASHES_QUERY,
                    String.class,
                    tenant, metricName, tagEntry.getKey(), tagEntry.getValue()
                )
                .doOnError(e -> readDbOperationErrorsCounter.increment())
                .collect(Collectors.toSet())
        )
        // and reduce to the intersection of those
        .reduce((results1, results2) ->
            results1.stream()
                .filter(results2::contains)
                .collect(Collectors.toSet())
        )
        .flatMapMany(Flux::fromIterable);
  }

  public Flux<TsdbQuery> locateSeriesSetHashesFromQuery(String tenant, TsdbQuery query) {
    return locateSeriesSetHashes(tenant, query.getMetricName(), query.getTags())
        .flatMap(seriesSet -> {
          query.setSeriesSet(seriesSet);
          return Flux.just(query);
        });
  }

  private TsdbQuery parseTsdbQuery(String metric, String downsample, String tagKey, String tagValue,
      List<Granularity> granularities) {
    Map<String, String> tags = Map.of(tagKey, tagValue);
    TsdbQuery tsdbQuery = new TsdbQuery();

    if (downsample != null && !downsample.isEmpty()) {
      String[] downsampleValues = downsample.split("-");

      Duration granularity =
          DateTimeUtils.getGranularity(Duration.parse("PT" + downsampleValues[0].toUpperCase()),
              granularities);

      tsdbQuery.setMetricName(metric)
          .setTags(tags)
          .setGranularity(granularity)
          .setAggregator(Aggregator.valueOf(downsampleValues[1]));
    } else {
      tsdbQuery.setMetricName(metric)
          .setTags(tags)
          .setGranularity(null)
          .setAggregator(Aggregator.raw);
    }
    return tsdbQuery;
  }

  public Flux<TsdbQuery> getTsdbQueries(
      List<TsdbQueryRequest> requests, List<Granularity> granularities) {
    List<TsdbQuery> result = new ArrayList<>();

    requests.forEach(request -> {

      if (request.getFilters() != null) {
        String metric = request.getMetric();
        String downsample = request.getDownsample();

        request.getFilters().forEach(filter -> {
          String tagKey = filter.getTagk();
          Arrays.stream(filter.getFilter().split("\\|"))
              .forEach(tagValue ->
                  result.add(parseTsdbQuery(metric, downsample, tagKey, tagValue, granularities)));
        });
      }
    });
    return Flux.fromIterable(result);
  }

  public MetricNameAndMultiTags getMetricNameAndTags(String m) {
    MetricNameAndMultiTags metricNameAndTags = new MetricNameAndMultiTags();
    List<Map<String, String>> tags = new ArrayList<>();
    if (m.matches(TAG_VALUE_REGEX)) {
      String tagKeys = m.substring(m.indexOf("{") + 1, m.indexOf("}"));
      String metricName = m.split("\\{")[0];
      Arrays.stream(tagKeys.split("\\,")).forEach(tagKey -> {
        String[] splitTag = tagKey.split("\\=");
        tags.add(Map.of(splitTag[0], splitTag[1]));
      });
      metricNameAndTags.setMetricName(metricName);
    } else {
      metricNameAndTags.setMetricName(m);
    }
    metricNameAndTags.setTags(tags);
    return metricNameAndTags;
  }

  public Mono<MetricNameAndTags> resolveSeriesSetHash(String tenant, String seriesSetHash) {
    return cassandraTemplate.selectOne(
            query(
                where(COL_TENANT).is(tenant),
                where(COL_SERIES_SET_HASH).is(seriesSetHash)
            ),
            SeriesSetHash.class
        )
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .switchIfEmpty(Mono.error(
            new IllegalStateException(
                "Unable to resolve series-set from hash \"" + seriesSetHash + "\"")
        ))
        .map(result ->
            new MetricNameAndTags()
                .setMetricName(result.getMetricName())
                .setTags(result.getTags())
        );

  }

  /**
   * This method is used to query data from tags_data table based on the SuggestType TAGK or TAGV.
   *
   * @param tenantId
   * @param type
   * @return
   */
  public Mono<List<String>> getTagKeysOrValuesForTenant(String tenantId, SuggestType type) {
    return cqlTemplate.queryForFlux(GET_TAG_KEYS_OR_VALUES_FROM_TAGS_DATA,
            String.class,
            tenantId,
            type.name()
        )
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .collectList();
  }
}
