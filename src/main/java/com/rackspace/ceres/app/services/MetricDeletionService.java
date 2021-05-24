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

package com.rackspace.ceres.app.services;


import static com.rackspace.ceres.app.web.TagListConverter.convertPairsListToMap;

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.helper.MetricDeletionHelper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks.Empty;

@Service
@Slf4j
@Profile("downsample")
public class MetricDeletionService {


  private final DataTablesStatements dataTablesStatements;
  private final MetadataService metadataService;
  private TimeSlotPartitioner timeSlotPartitioner;
  private DownsampleProperties downsampleProperties;
  private final MetricDeletionHelper metricDeletionHelper;

  private final String DELETE_METRIC_NAMES_QUERY = "DELETE FROM metric_names WHERE tenant = ? "
      + "AND metric_name = ?";
  private final String DELETE_SERIES_SET_QUERY = "DELETE FROM series_sets WHERE tenant = ? "
    + "AND metric_name = ?";
  private final String DELETE_SERIES_SET_HASHES_QUERY = "DELETE FROM series_set_hashes "
      + "WHERE tenant = ? AND series_set_hash = ?";
  private final String SELECT_SERIES_SET_HASHES_QUERY = "SELECT series_set_hash FROM series_sets "
      + "WHERE tenant = ? AND metric_name = ?";

  @Autowired
  public MetricDeletionService(DataTablesStatements dataTablesStatements,
      TimeSlotPartitioner timeSlotPartitioner, MetadataService metadataService,
      DownsampleProperties downsampleProperties,
      MetricDeletionHelper metricDeletionHelper) {
    this.dataTablesStatements = dataTablesStatements;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.downsampleProperties = downsampleProperties;
    this.metadataService = metadataService;
    this.metricDeletionHelper = metricDeletionHelper;
  }

  public Mono<Empty> deleteMetrics(String tenant, String metricName, List<String> tag,
      Instant start, Instant end) {
    if (StringUtils.isBlank(metricName)) {
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
            deleteMetrics(downsampleProperties.getGranularities(), tenant, timeSlot))
        .then(Mono.empty());
  }

  private Mono<Empty> deleteMetricsByMetricName(String tenant, String metricName, Instant start,
      Instant end) {
    log.debug("Deleting metrics {} for tenant: {} ", metricName,
        tenant);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashes = metricDeletionHelper.getSeriesSetHashFromSeriesSets(tenant,
              metricName);
          return deleteMetrics(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashes);
        }).then(metricDeletionHelper.deleteMetricNamesByTenantAndMetricName(tenant, metricName))
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
          return deleteMetrics(downsampleProperties.getGranularities(), tenant, timeSlot,
              metricName,
              metadataService.locateSeriesSetHashes(tenant, metricName, queryTags));
        }).then(Mono.empty());
  }

  /**
   * Delete metrics and its metadata by tenant.
   *
   * @param granularities the granularities
   * @param tenant        the tenant
   * @param timeSlot      the time slot
   * @return the mono
   */
  private Mono<Boolean> deleteMetrics(List<Granularity> granularities, String tenant,
      Instant timeSlot) {
    return deleteMetadataByTenantId(tenant, timeSlot)
        .then(Flux.fromIterable(granularities)
            .flatMap(granularity ->
                metricDeletionHelper.deleteRawOrDownsampledEntries(dataTablesStatements
                    .downsampleDelete(granularity.getWidth()), tenant, timeSlot))
            .then(metricDeletionHelper.deleteRawOrDownsampledEntries(dataTablesStatements.getRawDelete(),
                tenant, timeSlot)));
    )
  }

  /**
   * Delete metrics and its metadata by tenant, metricName and seriesSetHash
   *
   * @param granularities   the granularities
   * @param tenant          the tenant
   * @param timeSlot        the time slot
   * @param metricName      the metric name
   * @param seriesSetHashes the series set hashes
   * @return the mono
   */
  private Mono<Boolean> deleteMetrics(List<Granularity> granularities, String tenant,
      Instant timeSlot, String metricName, Flux<String> seriesSetHashes) {
    return seriesSetHashes.flatMap(seriesSetHash ->
        deleteMetricsByTenantIdAndSeriesSetHash(seriesSetHash, granularities, tenant, timeSlot)
            .then(deleteMetadataByTenantIdAndSeriesSet(seriesSetHash, tenant, metricName)))
        .then(Mono.just(true));
  }

  /**
   * Delete metrics by tenant and seriesSeHash.
   *
   * @param seriesSetHash the series set hash
   * @param granularities the granularities
   * @param tenant        the tenant
   * @param timeSlot      the time slot
   * @return the mono
   */
  private Mono<Boolean> deleteMetricsByTenantIdAndSeriesSetHash(String seriesSetHash,
      List<Granularity> granularities, String tenant, Instant timeSlot) {
    return Flux.fromIterable(granularities)
        .flatMap(granularity ->
            metricDeletionHelper.deleteRawOrDownsampledEntries(
                dataTablesStatements.downsampleDeleteWithSeriesSetHash(granularity.getWidth()),
                tenant, timeSlot, seriesSetHash))
        .then(metricDeletionHelper
            .deleteRawOrDownsampledEntries(dataTablesStatements.getRawDeleteWithSeriesSetHash(),
                tenant, timeSlot, seriesSetHash));
  }

  /**
   * Delete metadata by tenant id.
   *
   * @param tenant   the tenant
   * @param timeSlot the time slot
   * @return the mono
   */
  private Mono<Boolean> deleteMetadataByTenantId(String tenant, Instant timeSlot) {
    //get series set hashes from downsample table with max ttl by tenant and timeSlot
    Flux<String> seriesSetHashes = metricDeletionHelper.getSeriesSetHashFromRawOrDownsampled(tenant, timeSlot);
    //get metricNames from metric_names table by tenant
    Flux<String> metricNames = metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable);

    return seriesSetHashes.flatMap(seriesSetHash -> deleteSeriesSetHashesAndCache(tenant, seriesSetHash))
        .then(metricNames.flatMap(metricName -> deleteSeriesSetAndMetricName(tenant, metricName))
            .then(Mono.just(true)));
  }

  /**
   * Delete metadata by tenant, metricName and seriesSetHash.
   *
   * @param seriesSetHash the series set hash
   * @param tenant        the tenant
   * @param metricName    the metric name
   * @return the mono
   */
  private Mono<Boolean> deleteMetadataByTenantIdAndSeriesSet(String seriesSetHash, String tenant, String metricName) {
    return deleteSeriesSetHashesAndCache(tenant, seriesSetHash)
        .then(metricDeletionHelper.deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName));
  }


  /**
   * Delete seriesSetHashes and removed entry from cache.
   *
   * @param tenant        the tenant
   * @param seriesSetHash the series set hash
   * @return the mono
   */
  private Mono<Boolean> deleteSeriesSetHashesAndCache(String tenant, String seriesSetHash) {
    return metricDeletionHelper.deleteSeriesSetHashes(tenant, seriesSetHash)
        .then(metricDeletionHelper.removeEntryFromCache(tenant, seriesSetHash));
  }

  /**
   * Delete seriesSets and metricName.
   *
   * @param tenant     the tenant
   * @param metricName the metric name
   * @return the mono
   */
  private Mono<Boolean> deleteSeriesSetAndMetricName(String tenant, String metricName) {
    return metricDeletionHelper.deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName)
        .then(metricDeletionHelper.deleteMetricNamesByTenantAndMetricName(tenant, metricName));
  }
}
