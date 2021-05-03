package com.rackspace.ceres.app.services;


import static com.rackspace.ceres.app.web.TagListConverter.convertPairsListToMap;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

@Service
@Slf4j
@Profile("downsample")
public class MetricDeletionService {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String DELIM = "|";

  private final ReactiveCqlTemplate cqlTemplate;
  private final DataTablesStatements dataTablesStatements;
  private AppProperties appProperties;
  private TimeSlotPartitioner timeSlotPartitioner;
  private DownsampleProperties downsampleProperties;
  private final ReactiveStringRedisTemplate redisTemplate;

  @Autowired
  public MetricDeletionService(ReactiveCqlTemplate cqlTemplate,
      DataTablesStatements dataTablesStatements, AppProperties appProperties,
      TimeSlotPartitioner timeSlotPartitioner,
      DownsampleProperties downsampleProperties, ReactiveStringRedisTemplate redisTemplate) {
    this.cqlTemplate = cqlTemplate;
    this.dataTablesStatements = dataTablesStatements;
    this.appProperties = appProperties;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.downsampleProperties = downsampleProperties;
    this.redisTemplate = redisTemplate;
  }

  public Mono<Void> deleteMetrics(String tenant, String metricName, List<String> tag,
      Instant start, Instant end) {
    if (StringUtils.isAllBlank(metricName)) {
      return deleteMetricsByTenantId(tenant, start, end);
    } else if (CollectionUtils.isEmpty(tag)) {
      return deleteMetricsByMetricName(tenant, metricName, start, end);
    } else {
      return deleteMetricsByMetricNameAndTag(tenant, metricName, tag, start, end);
    }
  }

  private Mono<Void> deleteMetricsByTenantId(String tenant, Instant start, Instant end) {
    log.info("inside deleteMetricsByTenantId method with tenant {} ", tenant);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          return deleteMetricsByTenantId(downsampleProperties.getGranularities(), tenant, timeSlot);
        }).then();
  }

  private Mono<Void> deleteMetricsByMetricName(String tenant, String metricName, Instant start,
      Instant end) {
    log.info("inside deleteMetricsByMetricName method with tenant {}, metricName {} ", tenant,
        metricName);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashes = getSeriesSetHashFromSeriesSets(tenant,
              metricName);
          return deleteMetric(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashes);
        }).then();
  }

  private Mono<Void> deleteMetricsByMetricNameAndTag(String tenant, String metricName,
      List<String> tag, Instant start, Instant end) {
    log.info(
        "inside deleteMetricsByMetricNameAndTag method with tenant {}, metricName {} and tag {} ",
        tenant, metricName, tag);
    Map<String, String> queryTags = convertPairsListToMap(tag);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashes = getSeriesSetHashFromSeriesSets(tenant,
              metricName, queryTags);
          return deleteMetric(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashes);
        }).then();
  }

  private Mono<Boolean> deleteMetric(List<Granularity> granularities, String tenant,
      Instant timeSlot, String metricName, Flux<String> seriesSetHashes) {
    return seriesSetHashes.flatMap(
        seriesSetHash -> Flux.fromIterable(granularities)
            //delete entries from downsampled tables
            .flatMap(granularity ->
                deleteRawOrDownsampledEntries(
                    dataTablesStatements.downsampleDeleteWithSeriesSetHash(granularity.getWidth()),
                    tenant, timeSlot, seriesSetHash))
            .then(Mono.just(true))
    )
        .then(Mono.just(true))
        //delete entries from raw table
        .flatMap(item ->
            seriesSetHashes.flatMap(seriesSetHash ->
                deleteRawOrDownsampledEntries(dataTablesStatements.getRawDeleteWithSeriesSetHash(),
                    tenant, timeSlot, seriesSetHash))
                .then(Mono.just(true))
        )
        //delete entries from metric_names table
        .flatMap(result ->
            deleteMetricNamesByTenantAndMetricName(tenant, metricName)
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
    Flux<String> metricNames = getMetricNames(tenant);

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

  private Flux<String> getSeriesSetHashFromSeriesSets(String tenant,
      String metricName, Map<String, String> queryTags) {
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

  private Flux<String> getMetricNames(String tenant) {
    return cqlTemplate
        .queryForFlux("SELECT metric_name FROM metric_names WHERE tenant = ?", String.class,
            tenant);
  }

}

