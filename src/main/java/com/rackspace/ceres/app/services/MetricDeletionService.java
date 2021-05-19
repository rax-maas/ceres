package com.rackspace.ceres.app.services;


import static com.rackspace.ceres.app.web.TagListConverter.convertPairsListToMap;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks.Empty;

@Service
@Slf4j
@Profile("downsample")
public class MetricDeletionService {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String DELIM = "|";

  private final ReactiveCqlTemplate cqlTemplate;
  private final DataTablesStatements dataTablesStatements;
  private final MetadataService metadataService;
  private AppProperties appProperties;
  private TimeSlotPartitioner timeSlotPartitioner;
  private DownsampleProperties downsampleProperties;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache;

  private final String deleteMetricNamesQuery;
  private final String deleteSeriesSetQuery;
  private final String deleteSeriesSetHashesQuery;
  private final String selectSeriesSetHashesQuery;

  @Autowired
  public MetricDeletionService(ReactiveCqlTemplate cqlTemplate,
      DataTablesStatements dataTablesStatements, AppProperties appProperties,
      TimeSlotPartitioner timeSlotPartitioner, MetadataService metadataService,
      DownsampleProperties downsampleProperties, ReactiveStringRedisTemplate redisTemplate,
      AsyncCache<SeriesSetCacheKey, Boolean> seriesSetExistenceCache) {
    this.cqlTemplate = cqlTemplate;
    this.dataTablesStatements = dataTablesStatements;
    this.appProperties = appProperties;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.downsampleProperties = downsampleProperties;
    this.redisTemplate = redisTemplate;
    this.metadataService = metadataService;
    this.seriesSetExistenceCache = seriesSetExistenceCache;

    deleteMetricNamesQuery = "DELETE FROM metric_names WHERE tenant = ? AND metric_name = ?";
    deleteSeriesSetQuery = "DELETE FROM series_sets WHERE tenant = ? AND metric_name = ?";
    deleteSeriesSetHashesQuery = "DELETE FROM series_set_hashes WHERE tenant = ? AND series_set_hash = ?";
    selectSeriesSetHashesQuery = "SELECT series_set_hash FROM series_sets WHERE tenant = ? AND metric_name = ?";
  }

  public Mono<Empty> deleteMetrics(String tenant, String metricName, List<String> tag,
      Instant start, Instant end) {
    if (StringUtils.isAllBlank(metricName)) {
      return deleteMetricsByTenantId(tenant, start, end);
    } else if (CollectionUtils.isEmpty(tag)) {
      return deleteMetricsByMetricName(tenant, metricName, start, end);
    } else {
      return deleteMetricsByMetricNameAndTag(tenant, metricName, tag, start, end);
    }
  }

  private Mono<Empty> deleteMetricsByTenantId(String tenant, Instant start, Instant end) {
    log.debug("Deleting metrics for tenant: {}", tenant);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot ->
            deleteMetricsByTenantId(downsampleProperties.getGranularities(), tenant, timeSlot))
        .then(Mono.empty());
  }

  private Mono<Empty> deleteMetricsByMetricName(String tenant, String metricName, Instant start,
      Instant end) {
    log.debug("Deleting metrics {} for tenant: {} ", metricName,
        tenant);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashes = getSeriesSetHashFromSeriesSets(tenant,
              metricName);
          return deleteMetric(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashes);
        })
        //delete entries from metric_names table
        .flatMap(result ->
            deleteMetricNamesByTenantAndMetricName(tenant, metricName)
        )
        .then(Mono.empty());
  }

  private Mono<Empty> deleteMetricsByMetricNameAndTag(String tenant, String metricName,
      List<String> tag, Instant start, Instant end) {
    log.debug(
        "Deleting metrics {} with tag {} for tenant: {}  ",
        metricName, tag, tenant);
    Map<String, String> queryTags = convertPairsListToMap(tag);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashes = metadataService.locateSeriesSetHashes(tenant,
              metricName, queryTags);
          return deleteMetric(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashes);
        }).then(Mono.empty());
  }

  private Mono<Boolean> deleteMetric(List<Granularity> granularities, String tenant,
      Instant timeSlot, String metricName, Flux<String> seriesSetHashes) {
    return seriesSetHashes.flatMap(
        seriesSetHash -> Flux.fromIterable(granularities)
            //delete entries from downsampled tables
            .flatMap(granularity ->
                deleteRawOrDownsampledEntriesWithSeriesSetHash(
                    dataTablesStatements.downsampleDeleteWithSeriesSetHash(granularity.getWidth()),
                    tenant, timeSlot, seriesSetHash))
            .then(Mono.just(true))
    )
        .then(Mono.just(true))
        //delete entries from raw table
        .flatMap(item ->
            seriesSetHashes.flatMap(seriesSetHash ->
                deleteRawOrDownsampledEntriesWithSeriesSetHash(dataTablesStatements.getRawDeleteWithSeriesSetHash(),
                    tenant, timeSlot, seriesSetHash))
                .then(Mono.just(true))
        )
        //delete entries from series_set_hashes table
        .flatMap(result ->
            seriesSetHashes.flatMap(seriesSetHash ->
                deleteSeriesSetHashes(tenant, seriesSetHash)
            )
                .then(Mono.just(true)))
        //delete entries from redis cache
        .flatMap(result ->
            seriesSetHashes.flatMap(seriesSetHash ->
                removeEntryFromCache(tenant, seriesSetHash)
            )
                .then(Mono.just(true)))
        //delete entries from series_sets table
        .flatMap(result ->
            deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName)
        );
  }

  private Mono<Boolean> deleteMetricsByTenantId(List<Granularity> granularities, String tenant,
      Instant timeSlot) {
    //get series set hashes from data raw table by tenant and timeSlot
    Flux<String> seriesSetHashes = getSeriesSetHashFromRaw(tenant, timeSlot);
    //get metricNames from metric_names table by tenant
    Flux<String> metricNames = metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable);

    return Flux.fromIterable(granularities)
        .flatMap(granularity ->
            deleteRawOrDownsampledEntries(
                //delete entries from downsampled tables
                dataTablesStatements.downsampleDelete(granularity.getWidth()), tenant, timeSlot)
        )
        .then(Mono.just(true))
        //delete entries from series_set_hashes table
        .flatMap(result ->
            seriesSetHashes.flatMap(seriesSetHash ->
                deleteSeriesSetHashes(tenant, seriesSetHash)
            )
            .then(Mono.just(true))
        )
        //delete entries from series_sets table
        .flatMap(result ->
            metricNames.flatMap(metricName ->
                deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName)
            )
            .then(Mono.just(true))
        )
        //delete entries from metric_names table
        .flatMap(result ->
            metricNames.flatMap(metricName ->
                deleteMetricNamesByTenantAndMetricName(tenant, metricName)
            )
            .then(Mono.just(true))
        )
        //delete entries from redis cache
        .flatMap(result ->
            seriesSetHashes.flatMap(seriesSetHash ->
                removeEntryFromCache(tenant, seriesSetHash)
            )
            .then(Mono.just(true))
        )
        //delete entries from raw table
        .flatMap(item ->
            deleteRawOrDownsampledEntries(dataTablesStatements.getRawDelete(),
                tenant, timeSlot));
  }

  private Mono<Boolean> deleteMetricNamesByTenantAndMetricName(String tenant, String metricName) {
    return cqlTemplate
        .execute(deleteMetricNamesQuery, tenant, metricName)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteSeriesSetsByTenantIdAndMetricName(String tenant,
      String metricName) {
    return cqlTemplate
        .execute(deleteSeriesSetQuery, tenant, metricName)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteSeriesSetHashes(String tenant, String seriesSetHash) {
    return cqlTemplate
        .execute(deleteSeriesSetHashesQuery, tenant, seriesSetHash)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteRawOrDownsampledEntriesWithSeriesSetHash(String query, String tenant, Instant timeSlot,
      String seriesSetHash) {
    return cqlTemplate
        .execute(query, tenant, timeSlot,
            seriesSetHash)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteRawOrDownsampledEntries(String query, String tenant,
      Instant timeSlot) {
    return cqlTemplate
        .execute(query, tenant, timeSlot)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> removeEntryFromCache(String tenant, String seriesSetHash) {
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

  private Flux<String> getSeriesSetHashFromRaw(String tenant, Instant timeSlot) {
    return cqlTemplate
        .queryForFlux(dataTablesStatements.getRawGetHashSeriesSetHashQuery(), String.class, tenant, timeSlot);
  }

  private Flux<String> getSeriesSetHashFromSeriesSets(String tenant,
      String metricName) {
    return cqlTemplate.queryForFlux(selectSeriesSetHashesQuery, String.class,
        tenant, metricName).distinct();
  }
}
