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
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class MetadataService {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String DELIM = "|";
  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final Counter redisHit;
  private final Counter redisMiss;
  private final AppProperties appProperties;

  @Autowired
  public MetadataService(ReactiveCqlTemplate cqlTemplate,
                         ReactiveCassandraTemplate cassandraTemplate,
                         AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache,
                         ReactiveStringRedisTemplate redisTemplate,
                         MeterRegistry meterRegistry,
                         AppProperties appProperties) {
    this.cqlTemplate = cqlTemplate;
    this.cassandraTemplate = cassandraTemplate;
    this.seriesSetExistenceCache = seriesSetExistenceCache;
    this.redisTemplate = redisTemplate;

    redisHit = meterRegistry.counter("seriesSetHash.redisCache", "result", "hit");
    redisMiss = meterRegistry.counter("seriesSetHash.redisCache", "result", "miss");
    this.appProperties = appProperties;
  }

  public Publisher<?> storeMetadata(String tenant, String seriesSetHash,
                                    String metricName, Map<String, String> tags) {
    final CompletableFuture<Boolean> result = seriesSetExistenceCache.get(
        new SeriesSetCacheKey(tenant, seriesSetHash),
        (key, executor) ->
            redisTemplate.opsForValue()
                .setIfAbsent(
                    PREFIX_SERIES_SET_HASHES + DELIM + key.getTenant() + DELIM + key
                        .getSeriesSetHash(),
                    ""
                )
                .doOnNext(inserted -> {
                  if (inserted) {
                    redisMiss.increment();
                  } else {
                    redisHit.increment();
                  }
                })
                // already cached in redis?
                .flatMap(inserted -> !inserted ? Mono.just(true) :
                    // not cached, so store the metadata to be sure
                    storeMetadataInCassandra(tenant, seriesSetHash, metricName,
                        tags
                    )
                        .map(it -> true))
                .toFuture()
    );

    return Mono.fromFuture(result);
  }

  private Mono<?> storeMetadataInCassandra(String tenant, String seriesSetHash,
                                           String metricName, Map<String, String> tags) {
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
        );
  }

  public Mono<List<String>> getTenants() {
    return cqlTemplate.queryForFlux(
        "SELECT tenant FROM metric_names"
            // use GROUP BY since unable to SELECT DISTINCT on primary key column
            + " GROUP BY tenant",
        String.class
    ).collectList();
  }

  public Mono<List<String>> getMetricNames(String tenant) {
    return cqlTemplate.queryForFlux(
        "SELECT metric_name FROM metric_names WHERE tenant = ?",
        String.class,
        tenant
    ).collectList();
  }

  public Mono<List<String>> getTagKeys(String tenant, String metricName) {
    return cqlTemplate.queryForFlux(
        "SELECT tag_key FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ?"
            // use GROUP BY since unable to SELECT DISTINCT on primary key column
            + " GROUP BY tag_key",
        String.class,
        tenant, metricName
    ).collectList();
  }

  public Mono<List<String>> getTagValues(String tenant, String metricName, String tagKey) {
    return cqlTemplate.queryForFlux(
        "SELECT tag_value FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ? AND tag_key = ?"
            // use GROUP BY since unable to SELECT DISTINCT on primary key column
            + " GROUP BY tag_value",
        String.class,
        tenant, metricName, tagKey
    ).collectList();
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
            cqlTemplate.queryForFlux(
                "SELECT series_set_hash FROM series_sets"
                    + " WHERE tenant = ? AND metric_name = ? AND tag_key = ? AND tag_value = ?",
                String.class,
                tenant, metricName, tagEntry.getKey(), tagEntry.getValue()
            )
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


  public Mono<MetricNameAndTags> resolveSeriesSetHash(String tenant, String seriesSetHash) {
    return cassandraTemplate.selectOne(
        query(
            where(COL_TENANT).is(tenant),
            where(COL_SERIES_SET_HASH).is(seriesSetHash)
        ),
        SeriesSetHash.class
    )
        .switchIfEmpty(Mono.error(
            new IllegalStateException("Unable to resolve series-set from hash \""+seriesSetHash+"\"")
        ))
        .map(result ->
            new MetricNameAndTags()
                .setMetricName(result.getMetricName())
                .setTags(result.getTags())
        );

  }
}
