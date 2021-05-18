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
import org.reactivestreams.Publisher;
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

  private Mono<Boolean> deleteMetricsByTenantId(List<Granularity> granularities, String tenant,
      Instant timeSlot) {
    return deleteMetricByTenant(granularities, tenant, timeSlot)
        .then(deleteMetadataByTenant(tenant, timeSlot));
  }

  private Mono<Boolean> deleteMetricByTenant(List<Granularity> granularities, String tenant, Instant timeSlot) {
    return Flux.fromIterable(granularities)
        .flatMap(granularity ->
            deleteRawOrDownsampledEntries(dataTablesStatements
                .downsampleDelete(granularity.getWidth()), tenant, timeSlot))
        .then(deleteRawOrDownsampledEntries(dataTablesStatements.getRawDelete(),
            tenant, timeSlot));
  }


  private Mono<Boolean> deleteMetadataByTenant(String tenant, Instant timeSlot) {
    //get series set hashes from data raw table by tenant and timeSlot
    Flux<String> seriesSetHashes = getSeriesSetHashFromRaw(tenant, timeSlot);
    //get metricNames from metric_names table by tenant
    Flux<String> metricNames = metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable);

    return seriesSetHashes.flatMap(seriesSetHash -> deleteSeriesSetHashesAndCache(tenant, seriesSetHash))
        .then(metricNames.flatMap(metricName -> deleteSeriesSetAndMetricName(tenant, metricName))
            .then(Mono.just(true)));
  }

  private Mono<Boolean> deleteSeriesSetHashesAndCache(String tenant, String seriesSetHash) {
    return deleteSeriesSetHashes(tenant, seriesSetHash)
        .then(removeEntryFromCache(tenant, seriesSetHash));
  }

  private Mono<Boolean> deleteSeriesSetAndMetricName(String tenant, String metricName) {
    return deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName)
        .then(deleteMetricNamesByTenantAndMetricName(tenant, metricName));
  }


  private Mono<Boolean> deleteMetricNamesByTenantAndMetricName(String tenant, String metricName) {
    return cqlTemplate
        .execute("DELETE FROM metric_names WHERE tenant = ? AND metric_name = ?",
            tenant, metricName)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteSeriesSetsByTenantIdAndMetricName(String tenant,
      String metricName) {
    return cqlTemplate
        .execute("DELETE FROM series_sets WHERE tenant = ? AND metric_name = ?"
            , tenant, metricName)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteSeriesSetHashes(String tenant, String seriesSetHash) {
    return cqlTemplate
        .execute("DELETE FROM series_set_hashes WHERE tenant = ? AND series_set_hash = ?",
            tenant, seriesSetHash)
        .retryWhen(appProperties.getRetryDelete().build());
  }

  private Mono<Boolean> deleteRawOrDownsampledEntries(String query, String tenant, Instant timeSlot,
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
        .queryForFlux(dataTablesStatements.getRawGetHashQuery(), String.class, tenant, timeSlot);
  }

  private Flux<String> getSeriesSetHashFromSeriesSets(String tenant,
      String metricName) {
    return cqlTemplate.queryForFlux(
        "SELECT series_set_hash FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ? ",
        String.class,
        tenant, metricName).distinct();
  }

  private Mono<Boolean> deleteMetric(List<Granularity> granularities, String tenant,
      Instant timeSlot, String metricName, Flux<String> seriesSetHashes) {
    return
        seriesSetHashes.flatMap(seriesSetHash -> delete(seriesSetHash, granularities, tenant, timeSlot, metricName))
            .then(Mono.just(true));
  }

  private Mono<Boolean> delete(String seriesSetHash, List<Granularity> granularities, String tenant, Instant timeSlot, String metricName) {
    return deleteMetricData(seriesSetHash, granularities, tenant, timeSlot)
        .then(deleteMetadata(seriesSetHash, tenant, metricName));
  }

  private Mono<Boolean> deleteMetadata(String seriesSetHash, String tenant, String metricName) {
    return deleteSeriesSetHashes(tenant, seriesSetHash)
        .then(removeEntryFromCache(tenant, seriesSetHash))
        .then(deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName));
  }

  private Mono<Boolean> deleteMetricData(String seriesSetHash, List<Granularity> granularities, String tenant, Instant timeSlot) {
    return Flux.fromIterable(granularities)
        .flatMap(granularity ->
            deleteRawOrDownsampledEntries(dataTablesStatements.downsampleDeleteWithSeriesSetHash(granularity.getWidth()),
                tenant, timeSlot, seriesSetHash))
        .then(deleteRawOrDownsampledEntries(dataTablesStatements.getRawDeleteWithSeriesSetHash(), tenant, timeSlot, seriesSetHash));
  }
}
