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
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import com.rackspace.ceres.app.services.DataTablesStatements;
import com.rackspace.ceres.app.services.TimeSlotPartitioner;
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
  private final String DELETE_SERIES_SET_QUERY = "DELETE FROM series_sets WHERE tenant = ? "
      + "AND metric_name = ?";
  private final String DELETE_METRIC_NAMES_QUERY = "DELETE FROM metric_names WHERE tenant = ? "
      + "AND metric_name = ?";
  private final String DELETE_SERIES_SET_HASHES_QUERY = "DELETE FROM series_set_hashes "
      + "WHERE tenant = ? AND series_set_hash = ?";
  private final String SELECT_SERIES_SET_HASHES_QUERY = "SELECT series_set_hash FROM series_sets "
      + "WHERE tenant = ? AND metric_name = ?";

  private AppProperties appProperties;
  private final DataTablesStatements dataTablesStatements;
  private final TimeSlotPartitioner timeSlotPartitioner;
  private final ReactiveCqlTemplate cqlTemplate;
  private final AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final DownsampleProperties downsampleProperties;

  public MetricDeletionHelper(AppProperties appProperties,
      DataTablesStatements dataTablesStatements,
      ReactiveCqlTemplate cqlTemplate, ReactiveStringRedisTemplate redisTemplate,
      AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache,
      TimeSlotPartitioner timeSlotPartitioner, DownsampleProperties downsampleProperties) {
    this.appProperties = appProperties;
    this.dataTablesStatements = dataTablesStatements;
    this.cqlTemplate = cqlTemplate;
    this.seriesSetExistenceCache = seriesSetExistenceCache;
    this.redisTemplate = redisTemplate;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.downsampleProperties = downsampleProperties;
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
        .execute(DELETE_METRIC_NAMES_QUERY, tenant, metricName)
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
        .execute(DELETE_SERIES_SET_QUERY, tenant, metricName)
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
        .execute(DELETE_SERIES_SET_HASHES_QUERY, tenant, seriesSetHash)
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
   * @param tenant the tenant
   * @param start  the start
   * @param end    the end
   * @return the series set hash from downsampled
   */
  public Flux<String> getSeriesSetHashFromRawOrDownsampled(String tenant, Instant start, Instant end) {
    return getSeriesSetHashFromRaw(tenant, start, end)
        .switchIfEmpty(getSeriesSetHashFromDownsampled(tenant, start, end));
  }

  /**
   * Gets series set hash from raw table.
   *
   * @param tenant the tenant
   * @param start  the start
   * @param end    the end
   * @return the series set hash from raw
   */
  private Flux<String> getSeriesSetHashFromRaw(String tenant, Instant start, Instant end) {
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> cqlTemplate
            .queryForFlux(dataTablesStatements.getRawGetHashSeriesSetHashQuery(), String.class, tenant, timeSlot));
  }

  /**
   * Gets series set hash from downsampled table.
   *
   * @param tenant the tenant
   * @param start  the start
   * @param end    the end
   * @return the series set hash from downsampled
   */
  private Flux<String> getSeriesSetHashFromDownsampled(String tenant, Instant start, Instant end) {
    return Flux.fromIterable(downsampleProperties.getGranularities()).flatMap(granularity -> Flux.fromIterable(
        timeSlotPartitioner.partitionsOverRange(start, end, granularity.getWidth()))
        .flatMap(timeSlot -> cqlTemplate
            .queryForFlux(dataTablesStatements.getDownsampledGetHashQuery(), String.class,
                tenant, timeSlot)));
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
    return cqlTemplate.queryForFlux(SELECT_SERIES_SET_HASHES_QUERY, String.class,
        tenant, metricName).distinct();
  }
}
