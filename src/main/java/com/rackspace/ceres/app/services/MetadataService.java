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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.entities.TagsData;
import com.rackspace.ceres.app.model.*;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.rackspace.ceres.app.entities.SeriesSetHash.COL_SERIES_SET_HASH;
import static com.rackspace.ceres.app.entities.SeriesSetHash.COL_TENANT;
import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

@Service
@Slf4j
public class MetadataService {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String LABEL_METRIC_GROUP = "metricGroup";
  private static final String LABEL_RESOURCE = "resource";
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

  // use GROUP BY since unable to SELECT DISTINCT on primary key column
  private static final String GET_TENANT_QUERY = "SELECT tenant FROM metric_names GROUP BY tenant";
  private static final String GET_METRIC_NAMES_QUERY =
      "SELECT metric_name FROM metric_names WHERE tenant = ?";
  private static final String GET_METRIC_GROUPS_QUERY =
      "SELECT metric_group FROM metric_groups WHERE tenant = ?";
  private static final String GET_TAG_KEY_QUERY = "SELECT tag_key FROM series_sets"
      + " WHERE tenant = ? AND metric_name = ? GROUP BY tag_key";
  private static final String GET_TAG_VALUE_QUERY = "SELECT tag_value FROM series_sets"
      + " WHERE tenant = ? AND metric_name = ? AND tag_key = ? GROUP BY tag_value";
  private static final String GET_SERIES_SET_HASHES_QUERY = "SELECT series_set_hash "
      + "FROM series_sets WHERE tenant = ? AND metric_name = ? AND tag_key = ? AND tag_value = ?";
  private static final String GET_METRIC_NAMES_FROM_METRIC_GROUP_QUERY =
      "SELECT metric_names FROM metric_groups WHERE tenant = ? AND metric_group = ?";
  private static final String UPDATE_METRIC_GROUP_ADD_METRIC_NAME =
      "UPDATE metric_groups SET metric_names = metric_names + {'%s'}, updated_at = '%s' WHERE "
          + "tenant = '%s' AND metric_group = '%s'";
  private static final String UPDATE_DEVICE_ADD_METRIC_NAME =
      "UPDATE devices SET metric_names = metric_names + {'%s'}, updated_at = '%s' WHERE "
          + "tenant = '%s' AND device = '%s'";
  private static final String GET_DEVICES_PER_TENANT_QUERY =
      "SELECT device FROM devices WHERE tenant = ?";
  private static final String GET_METRIC_NAMES_FROM_DEVICE_QUERY =
      "SELECT metric_names FROM devices WHERE tenant = ? AND device = ?";
  private static final String GET_TAG_KEYS_OR_VALUES_FROM_TAGS_DATA = "SELECT data from tags_data where tenant = ? AND type = ?";

  @Autowired
  public MetadataService(ReactiveCqlTemplate cqlTemplate,
                         ReactiveCassandraTemplate cassandraTemplate,
                         AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache,
                         MeterRegistry meterRegistry,
                         AppProperties appProperties) {
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
                            .doOnError(t -> log.error("Exception in exists check "+t.getMessage()))
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
                                            .map(it -> true))
                            .toFuture()
    );
    return Mono.fromFuture(result);
  }

  public Mono<?> updateMetricGroupAddMetricName(
      String tenant, String metricGroup, String metricName, String updatedAt) {
    if(metricGroup == null || metricGroup.isEmpty()) {
      return Mono.empty();
    }
    return cqlTemplate.execute(String.format(
        UPDATE_METRIC_GROUP_ADD_METRIC_NAME, metricName, updatedAt, tenant, metricGroup))
        .doOnError(e -> writeDbOperationErrorsCounter.increment());
  }

  private Mono<?> storeMetadataInCassandra(String tenant, String seriesSetHash, Metric metric) {
    String updatedAt = metric.getTimestamp().toString();
    String metricGroup = metric.getTags().get(LABEL_METRIC_GROUP);
    String device = metric.getTags().get(LABEL_RESOURCE);
    String metricName = metric.getMetric();
    Map<String,String> tags = metric.getTags();
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
            .and(this.updateMetricGroupAddMetricName(tenant, metricGroup, metricName, updatedAt)).retryWhen(
                    appProperties.getRetryInsertMetadata().build()
            )
            .and(this.updateDeviceAddMetricName(tenant, device, metricName, updatedAt)).retryWhen(
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
                                    ),
                                //Insert tenant-tagKeys mapping in TagsData table.
                                cassandraTemplate.insert(
                                    new TagsData()
                                          .setTenant(tenant)
                                          .setType(SuggestType.TAGK)
                                          .setData(tagsEntry.getKey())
                                )
                                    .retryWhen(
                                        appProperties.getRetryInsertMetadata().build()
                                    ),
                                //Insert tenant-tagValues mapping in TagsData table.
                                cassandraTemplate.insert(
                                    new TagsData()
                                        .setTenant(tenant)
                                        .setType(SuggestType.TAGV)
                                        .setData(tagsEntry.getValue())
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

  public Mono<List<String>> getMetricGroups(String tenant) {
    return cqlTemplate.queryForFlux(GET_METRIC_GROUPS_QUERY,
        String.class,
        tenant
    )
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .collectList();
  }

  public Flux<String> getMetricNamesFromMetricGroup(String tenant, String metricGroup) {
    return cqlTemplate.queryForRows(GET_METRIC_NAMES_FROM_METRIC_GROUP_QUERY, tenant, metricGroup)
        .flatMap(row -> Flux.fromIterable(row.getSet("metric_names", String.class)))
        .doOnError(e -> readDbOperationErrorsCounter.increment());
  }

  public Flux<Map<String, String>> getTags(String tenantHeader, String metricName) {
    return this.getTagKeys(tenantHeader, metricName)
            .flatMapMany(Flux::fromIterable)
            .flatMap(tag -> this.getTagValueMaps(tag, tenantHeader, metricName)
                    .flatMapMany(Flux::fromIterable)
                    .flatMap(Mono::just)
            );
  }

  public Mono<List<String>> getTagKeys(String tenant, String metricName) {
    return getTagKeysRaw(tenant, metricName).collectList();
  }

  public Mono<List<String>> getTagValues(String tenant, String metricName, String tagKey) {
    return getTagValuesRaw(tenant, metricName, tagKey).collectList();
  }

  private Flux<String> getTagKeysRaw(String tenant, String metricName) {
    return cqlTemplate.queryForFlux(GET_TAG_KEY_QUERY,
            String.class,
            tenant, metricName
    ).doOnError(e -> readDbOperationErrorsCounter.increment());
  }

  private Mono<List<Map<String, String>>> getTagValueMaps(String tagKey, String tenantHeader, String metricName) {
    return getTagValuesRaw(tenantHeader, metricName, tagKey)
            .map(tagValue -> Map.of(tagKey, tagValue)).collectList();
  }

  private Flux<String> getTagValuesRaw(String tenant, String metricName, String tagKey) {
    return cqlTemplate.queryForFlux(GET_TAG_VALUE_QUERY,
            String.class,
            tenant, metricName, tagKey
    ).doOnError(e -> readDbOperationErrorsCounter.increment());
  }

  /**
   * Locates the recorded series-sets (<code>metricName,tagK=tagV,...</code>) that match the given
   * search criteria.
   * @param tenant series-sets are located by this tenant
   * @param metricName series-sets are located by this metric name
   * @param queryTags series-sets are located by and'ing these tag key-value pairs
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
            new IllegalStateException("Unable to resolve series-set from hash \""+seriesSetHash+"\"")
        ))
        .map(result ->
            new MetricNameAndTags()
                .setMetricName(result.getMetricName())
                .setTags(result.getTags())
        );

  }


  /**
   * Returns Tags based upon tenantId and metricName/metricGroup
   *
   * @param tenantHeader the tenant header
   * @param metricName   the metric name
   * @param metricGroup  the metric group
   * @return the tags
   */
  public Mono<TagsResponse> getTags(String tenantHeader, String metricName,
      String metricGroup) {
    HashMap<String, String> tags = new HashMap<>();
    if (StringUtils.hasText(metricName)) {
      return getTags(tenantHeader, metricName).map(e -> {
        tags.putAll(e);
        return tags;
      }).then(Mono.just(buildTagsResponse(tenantHeader, tags).setMetric(metricName)));
    } else {
      return getMetricNamesFromMetricGroup(tenantHeader, metricGroup)
          .flatMap(metricName1 -> getTags(tenantHeader, metricName1)).map(e -> {
            tags.putAll(e);
            return tags;
          }).then(Mono.just(buildTagsResponse(tenantHeader, tags).setMetricGroup(metricGroup)));
    }
  }

  private TagsResponse buildTagsResponse(String tenantHeader, HashMap<String, String> tags) {
    return new TagsResponse().setTags(tags)
        .setTenantId(tenantHeader);
  }

  public Mono<Boolean> updateDeviceAddMetricName(
      String tenant, String device, String metricName, String updatedAt) {
    if(device == null || device.isEmpty()) {
      return Mono.just(false);
    }
    return cqlTemplate.execute(String.format(
        UPDATE_DEVICE_ADD_METRIC_NAME, metricName, updatedAt, tenant, device))
        .doOnError(e -> writeDbOperationErrorsCounter.increment());
  }

  public Mono<List<String>> getDevices(String tenant) {
    return cqlTemplate.queryForFlux(GET_DEVICES_PER_TENANT_QUERY,
        String.class,
        tenant
    )
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .collectList();
  }

  public Mono<List<String>> getMetricNamesFromDevice(String tenantHeader, String device) {
    return cqlTemplate.queryForRows(GET_METRIC_NAMES_FROM_DEVICE_QUERY, tenantHeader, device)
        .flatMap(row -> Flux.fromIterable(row.getSet("metric_names", String.class)))
        .doOnError(e -> readDbOperationErrorsCounter.increment())
        .collectList();
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
