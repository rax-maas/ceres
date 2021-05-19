/*
 * Copyright 2021 Rackspace US, Inc.
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
 *
 */

package com.rackspace.ceres.app.helper;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import com.rackspace.ceres.app.services.DataTablesStatements;
import java.time.Instant;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
public class MetricDeletionHelper {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String DELIM = "|";

  private AppProperties appProperties;
  private final DataTablesStatements dataTablesStatements;
  private final ReactiveCqlTemplate cqlTemplate;
  private final AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache;
  private final ReactiveStringRedisTemplate redisTemplate;

  public MetricDeletionHelper(AppProperties appProperties,
      DataTablesStatements dataTablesStatements,
      ReactiveCqlTemplate cqlTemplate, ReactiveStringRedisTemplate redisTemplate,
      AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache) {
    this.appProperties = appProperties;
    this.dataTablesStatements = dataTablesStatements;
    this.cqlTemplate = cqlTemplate;
    this.seriesSetExistenceCache = seriesSetExistenceCache;
    this.redisTemplate = redisTemplate;
  }

  /**
   * Delete metric_names by tenant and metric name.
   *
   * @param tenant     the tenant
   * @param metricName the metric name
   * @return the mono
   */
  public Mono<Boolean> deleteMetricNamesByTenantAndMetricName(String tenant, String metricName) {
    return cqlTemplate
        .execute("DELETE FROM metric_names WHERE tenant = ? AND metric_name = ?",
            tenant, metricName)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  /**
   * Delete series_sets by tenant id and metric name.
   *
   * @param tenant     the tenant
   * @param metricName the metric name
   * @return the mono
   */
  public Mono<Boolean> deleteSeriesSetsByTenantIdAndMetricName(String tenant,
      String metricName) {
    return cqlTemplate
        .execute("DELETE FROM series_sets WHERE tenant = ? AND metric_name = ?"
            , tenant, metricName)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  /**
   * Delete series_set_hashes by tenant and seriesSetHash.
   *
   * @param tenant        the tenant
   * @param seriesSetHash the series set hash
   * @return the mono
   */
  public Mono<Boolean> deleteSeriesSetHashes(String tenant, String seriesSetHash) {
    return cqlTemplate
        .execute("DELETE FROM series_set_hashes WHERE tenant = ? AND series_set_hash = ?",
            tenant, seriesSetHash)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  /**
   * Delete metrics from raw or downsampled tables by tenant, timeslot and seriesSetHash.
   *
   * @param query         the query
   * @param tenant        the tenant
   * @param timeSlot      the time slot
   * @param seriesSetHash the series set hash
   * @return the mono
   */
  public Mono<Boolean> deleteRawOrDownsampledEntries(String query, String tenant, Instant timeSlot,
      String seriesSetHash) {
    return cqlTemplate
        .execute(query, tenant, timeSlot,
            seriesSetHash)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  /**
   * Delete metrics from raw or downsampled tables by tenant and timeslot.
   *
   * @param query    the query
   * @param tenant   the tenant
   * @param timeSlot the time slot
   * @return the mono
   */
  public Mono<Boolean> deleteRawOrDownsampledEntries(String query, String tenant,
      Instant timeSlot) {
    return cqlTemplate
        .execute(query, tenant, timeSlot)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  /**
   * Remove entry from local cache as well as redis
   *
   * @param tenant        the tenant
   * @param seriesSetHash the series set hash
   * @return the mono
   */
  public Mono<Boolean> removeEntryFromCache(String tenant, String seriesSetHash) {
    seriesSetExistenceCache.synchronous()
        .invalidate(new SeriesSetCacheKey(tenant, seriesSetHash));

    return redisTemplate.delete(PREFIX_SERIES_SET_HASHES + DELIM + tenant + DELIM + seriesSetHash)
        .flatMap(result -> {
          if (result > 0) {
            return Mono.just(true);
          } else {
            return Mono.just(false);
          }
        });
  }

  /**
   * Gets series_set_hash from raw or downsampled tables
   *
   * @param tenant   the tenant
   * @param timeSlot the time slot
   * @return the series set hash from downsampled
   */
  public Flux<String> getSeriesSetHashFromRawOrDownsampled(String tenant, Instant timeSlot) {
    return cqlTemplate
        .queryForFlux(dataTablesStatements.getRawGetHashQuery(), String.class, tenant, timeSlot)
        .switchIfEmpty(cqlTemplate
            .queryForFlux(dataTablesStatements.getDownsampledGetHashQuery(), String.class, tenant, timeSlot));
  }

  /**
   * Gets seriesSetHashes from series sets table by tenant and metricName
   *
   * @param tenant     the tenant
   * @param metricName the metric name
   * @return the series set hash from series sets
   */
  public Flux<String> getSeriesSetHashFromSeriesSets(String tenant,
      String metricName) {
    return cqlTemplate.queryForFlux(
        "SELECT series_set_hash FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ? ",
        String.class,
        tenant, metricName).distinct();
  }
}
