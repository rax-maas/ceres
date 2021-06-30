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
      Instant start, Instant end, String metricGroup) {
    if(StringUtils.isNotBlank(metricGroup)) {
      return deleteMetricsByMetricGroup(tenant, metricGroup, start, end);
    }
    else if (StringUtils.isBlank(metricName)) {
      return deleteMetricsByTenantId(tenant, start, end);
    } else if (CollectionUtils.isEmpty(tag)) {
      return deleteMetricsByMetricName(tenant, metricName, start, end);
    } else {
      return deleteMetricsByMetricNameAndTag(tenant, metricName, tag, start, end);
    }
  }

  /**
   * Deletes a metric by metric group.
   *
   * @param tenant
   * @param metricGroup
   * @param start
   * @param end
   * @return
   */
  private Mono<Empty> deleteMetricsByMetricGroup(String tenant, String metricGroup, Instant start,
      Instant end) {
    return metadataService.getMetricNamesFromMetricGroup(tenant, metricGroup)
        .flatMap(metricName -> deleteMetricsByMetricName(tenant, metricName, start, end))
        .then(metricDeletionHelper.deleteMetricGroupByTenantAndMetricGroup(tenant, metricGroup))
        .then(Mono.empty());
  }

  /**
   * Delete metrics by tenant id.
   *
   * @param tenant the tenant
   * @param start  the start
   * @param end    the end
   * @return the mono
   */
  private Mono<Empty> deleteMetricsByTenantId(String tenant, Instant start, Instant end) {
    log.debug("Deleting metrics for tenant: {}", tenant);
    Flux<String> seriesSetHashes = metricDeletionHelper
        .getSeriesSetHashFromRawOrDownsampled(tenant, start, end);
    return deleteMetadataByTenantId(tenant, seriesSetHashes)
        .then(deleteMetricsFromDownsampled(tenant, start, end))
        .then(deleteMetricsFromRaw(tenant, start, end))
        .then(Mono.empty());
  }

  /**
   * Delete metrics by metric name.
   *
   * @param tenant     the tenant
   * @param metricName the metric name
   * @param start      the start
   * @param end        the end
   * @return the mono
   */
  private Mono<Empty> deleteMetricsByMetricName(String tenant, String metricName, Instant start,
      Instant end) {
    log.debug("Deleting metrics {} for tenant: {} ", metricName,
        tenant);
    Flux<String> seriesSetHashes = metricDeletionHelper.getSeriesSetHashFromSeriesSets(tenant,
        metricName);
    return deleteMetrics(tenant, start, end, metricName, seriesSetHashes)
        .then(metricDeletionHelper.deleteMetricNamesByTenantAndMetricName(tenant, metricName))
        .then(Mono.empty());
  }

  /**
   * Delete metrics by metric name and tag.
   *
   * @param tenant     the tenant
   * @param metricName the metric name
   * @param tag        the tag
   * @param start      the start
   * @param end        the end
   * @return the mono
   */
  private Mono<Empty> deleteMetricsByMetricNameAndTag(String tenant, String metricName,
      List<String> tag, Instant start, Instant end) {
    log.debug(
        "Deleting metrics {} with tag {} for tenant: {}  ",
        metricName, tag, tenant);
    Map<String, String> queryTags = convertPairsListToMap(tag);
    Flux<String> seriesSetHashes =
        metadataService.locateSeriesSetHashes(tenant, metricName, queryTags);
    return deleteMetrics(tenant, start, end, metricName, seriesSetHashes)
        .then(Mono.empty());
  }

  /**
   * Delete metrics.
   *
   * @param tenant          the tenant
   * @param start           the start
   * @param end             the end
   * @param metricName      the metric name
   * @param seriesSetHashes the series set hashes
   * @return the mono
   */
  private Mono<Boolean> deleteMetrics(String tenant, Instant start, Instant end,
      String metricName, Flux<String> seriesSetHashes) {
    return seriesSetHashes.flatMap(seriesSetHash ->
        deleteMetricsFromDownsampledWithSeriesSetHash(tenant, start, end, seriesSetHash)
            .then(deleteMetricsFromRawWithSeriesSetHash(tenant, start, end, seriesSetHash))
            .then(deleteMetadataByTenantIdAndSeriesSet(seriesSetHash, tenant, metricName)))
        .then(Mono.just(true));
  }

  /**
   * Delete metadata by tenant id.
   *
   * @param tenant          the tenant
   * @param seriesSetHashes the series set hashes
   * @return the mono
   */
  private Mono<Boolean> deleteMetadataByTenantId(String tenant, Flux<String> seriesSetHashes) {
    //get metricNames from metric_names table by tenant
    Flux<String> metricNames = metadataService.getMetricNames(tenant)
        .flatMapMany(Flux::fromIterable);

    return seriesSetHashes.flatMap(seriesSetHash ->
        deleteSeriesSetHashesAndCache(tenant, seriesSetHash))
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
  private Mono<Boolean> deleteMetadataByTenantIdAndSeriesSet(String seriesSetHash, String tenant,
      String metricName) {
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

  /**
   * Delete metrics from downsampled table.
   *
   * @param tenant the tenant
   * @param start  the start
   * @param end    the end
   * @return the mono
   */
  private Mono<Boolean> deleteMetricsFromDownsampled(String tenant, Instant start, Instant end) {
    return Flux.fromIterable(downsampleProperties.getGranularities()).flatMap(granularity -> Flux
        .fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, granularity.getWidth()))
        .flatMap(timeSlot -> metricDeletionHelper.deleteRawOrDownsampledEntries(dataTablesStatements
            .downsampleDelete(granularity.getWidth()), tenant, timeSlot))).then(Mono.just(true));
  }

  /**
   * Delete metrics from raw table.
   *
   * @param tenant the tenant
   * @param start  the start
   * @param end    the end
   * @return the mono
   */
  private Mono<Boolean> deleteMetricsFromRaw(String tenant, Instant start, Instant end) {
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot ->
            metricDeletionHelper.deleteRawOrDownsampledEntries(dataTablesStatements.getRawDelete()
                , tenant, timeSlot))
        .then(Mono.just(true));
  }

  /**
   * Delete metrics from downsampled tables with series set hash.
   *
   * @param tenant        the tenant
   * @param start         the start
   * @param end           the end
   * @param seriesSetHash the series set hash
   * @return the mono
   */
  private Mono<Boolean> deleteMetricsFromDownsampledWithSeriesSetHash(String tenant, Instant start,
      Instant end, String seriesSetHash) {
    return Flux.fromIterable(downsampleProperties.getGranularities()).flatMap(granularity -> Flux
        .fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, granularity.getWidth()))
        .flatMap(timeSlot -> metricDeletionHelper.deleteRawOrDownsampledEntries(dataTablesStatements
                .downsampleDeleteWithSeriesSetHash(granularity.getWidth()), tenant, timeSlot,
            seriesSetHash))).then(Mono.just(true));
  }

  /**
   * Delete metrics from raw tables with series set hash.
   *
   * @param tenant        the tenant
   * @param start         the start
   * @param end           the end
   * @param seriesSetHash the series set hash
   * @return the mono
   */
  private Mono<Boolean> deleteMetricsFromRawWithSeriesSetHash(String tenant, Instant start,
      Instant end, String seriesSetHash) {
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot ->
            metricDeletionHelper
                .deleteRawOrDownsampledEntries(dataTablesStatements.getRawDeleteWithSeriesSetHash()
                    , tenant, timeSlot, seriesSetHash))
        .then(Mono.just(true));
  }
}
