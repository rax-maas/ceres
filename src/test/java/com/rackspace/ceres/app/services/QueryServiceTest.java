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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.model.FilterType;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.model.TsdbFilter;
import com.rackspace.ceres.app.model.TsdbQuery;
import com.rackspace.ceres.app.model.TsdbQueryRequest;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

@SpringBootTest
@ActiveProfiles(profiles = {"test", "downsample","query"})
@Testcontainers
@Slf4j
class QueryServiceTest {

  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>(
      CassandraContainerSetup.DOCKER_IMAGE);

  @TestConfiguration
  @Import(CassandraContainerSetup.class)
  public static class TestConfig {
    @Bean
    CassandraContainer<?> cassandraContainer() {
      return cassandraContainer;
    }
  }

  static {
    GenericContainer redis = new GenericContainer("redis:3-alpine")
        .withExposedPorts(6379);
    redis.start();

    System.setProperty("spring.redis.host", redis.getContainerIpAddress());
    System.setProperty("spring.redis.port", redis.getFirstMappedPort() + "");
  }

  @MockBean
  MetadataService metadataService;

  @Autowired
  QueryService queryService;

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Autowired
  DataWriteService dataWriteService;

  @Autowired
  SeriesSetService seriesSetService;

  @MockBean
  IngestTrackingService ingestTrackingService;

  @Autowired
  DownsampleProcessor downsampleProcessor;
  @Autowired
  DownsamplingService downsamplingService;
  @Autowired
  DownsampleProperties downsampleProperties;

  @AfterEach
  void tearDown() {
    cassandraTemplate.truncate(MetricName.class)
        .and(cassandraTemplate.truncate(SeriesSet.class))
        .subscribe();
  }

  @Test
  void testQueryRawWithMetricName() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String resource = RandomStringUtils.randomAlphabetic(5);
    final String monitoring_system = RandomStringUtils.randomAlphanumeric(5);

    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "metricGroup", metricGroup,
        "resource", resource,
        "monitoring_system", monitoring_system
    );
    final String seriesSetHash = seriesSetService
        .hash(metricName, tags);

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.storeMetadata(any(), any(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any()))
        .thenReturn(Flux.just(seriesSetHash));

    MetricNameAndTags metricNameAndTags = new MetricNameAndTags().setTags(tags).setMetricName(metricName);
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(metricNameAndTags));

    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());
    when(metadataService.updateDeviceAddMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(Instant.now())
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    StepVerifier.create(queryService
            .queryRaw(tenantId, metricName, "", tags, Instant.now().minusSeconds(60), Instant.now()).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getData().getMetricName()).isEqualTo(metricName);
          assertThat(result.get(0).getData().getTenant()).isEqualTo(tenantId);
          assertThat(result.get(0).getData().getTags()).isEqualTo(tags);
        }).verifyComplete();
  }

  @Test
  void testQueryRawWithMetricGroup() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String resource = RandomStringUtils.randomAlphabetic(5);
    final String monitoring_system = RandomStringUtils.randomAlphanumeric(5);

    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "metricGroup", metricGroup,
        "resource", resource,
        "monitoring_system", monitoring_system
    );
    final String seriesSetHash = seriesSetService
        .hash(metricName, tags);

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.storeMetadata(any(), any(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any()))
        .thenReturn(Flux.just(seriesSetHash));

    MetricNameAndTags metricNameAndTags = new MetricNameAndTags().setTags(tags).setMetricName(metricName);
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(metricNameAndTags));
    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateDeviceAddMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());
    when(metadataService.getMetricNamesFromMetricGroup(anyString(), anyString())).thenReturn(Flux.just(metricName));

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(Instant.now())
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    StepVerifier.create(queryService
            .queryRaw(tenantId, "", metricGroup, tags, Instant.now().minusSeconds(60), Instant.now()).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getData().getMetricName()).isEqualTo(metricName);
          assertThat(result.get(0).getData().getTenant()).isEqualTo(tenantId);
          assertThat(result.get(0).getData().getTags()).isEqualTo(tags);
        }).verifyComplete();
  }

  @Test
  void testQueryRawWithSeriesSet() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String resource = RandomStringUtils.randomAlphabetic(5);
    final String monitoring_system = RandomStringUtils.randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "metricGroup", metricGroup,
        "resource", resource,
        "monitoring_system", monitoring_system
    );
    final String seriesSetHash = seriesSetService
        .hash(metricName, tags);

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.storeMetadata(any(), any(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());
    when(metadataService.updateDeviceAddMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());
    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any()))
        .thenReturn(Flux.just(seriesSetHash));

    MetricNameAndTags metricNameAndTags = new MetricNameAndTags().setTags(tags).setMetricName(metricName);
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(metricNameAndTags));

    Instant instant = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(instant)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    StepVerifier.create(queryService
            .queryRawWithSeriesSet(tenantId, seriesSetHash, Instant.now().minusSeconds(60), Instant.now()).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getTimestamp()).isEqualTo(instant.truncatedTo(ChronoUnit.MILLIS));
        }).verifyComplete();
  }

  @Test
  void testQueryDownsampledWithMetricName() {
    final String tenant = randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "resource", "cpu-12",
        "metricGroup", metricGroup
    );

    final String seriesSetHash = seriesSetService.hash(metricName, tags);
    Instant now = Instant.now();
    String group = "PT15M";

    when(ingestTrackingService.track(any(), anyString(), any())).thenReturn(Mono.empty());
    when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateDeviceAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any())).thenReturn(Flux.just(seriesSetHash));
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(new MetricNameAndTags().setTags(tags).setMetricName(metricName)));
    when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());

    dataWriteService.ingest(Flux.fromIterable(List.of(
        Tuples.of(tenant, new Metric().setTimestamp(now).setValue(1.2).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(5)).setValue(1.5).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(10)).setValue(1.1).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(15)).setValue(3.4).setMetric(metricName).setTags(tags))
    ))).subscribe();

    final Long normalizedTimeSlot = DateTimeUtils.normalizedTimeslot(now, group);
    final PendingDownsampleSet pendingSet = new PendingDownsampleSet()
        .setSeriesSetHash(seriesSetHash)
        .setTenant(tenant)
        .setTimeSlot(Instant.ofEpochSecond(normalizedTimeSlot));

    DateTimeUtils.filterGroupGranularities(group, downsampleProperties.getGranularities()).forEach(granularity ->
        this.downsamplingService.downsampleData(
            pendingSet,
            granularity.getWidth(),
            queryService.fetchData(pendingSet, group, granularity.getWidth(), false)
        ).subscribe()
    );

    StepVerifier.create(queryService.queryDownsampled(tenant, metricName, "", Aggregator.min, Duration.ofMinutes(15), tags,
            now.minusSeconds(60 * 60), now.plusSeconds(24 * 60 * 60)).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getData().getTenant()).isEqualTo(tenant);
          assertThat(result.get(0).getData().getMetricName()).isEqualTo(metricName);
          assertThat(result.get(0).getData().getTags()).isEqualTo(tags);
        }).verifyComplete();
  }

  @Test
  void testQueryDownsampledWithMetricGroup() {
    final String tenant = randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "resource", "cpu-12",
        "metricGroup", metricGroup
    );

    final String seriesSetHash = seriesSetService.hash(metricName, tags);
    Instant now = Instant.now();
    String group = "PT15M";

    when(ingestTrackingService.track(any(), anyString(), any())).thenReturn(Mono.empty());
    when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateDeviceAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any())).thenReturn(Flux.just(seriesSetHash));
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(new MetricNameAndTags().setTags(tags).setMetricName(metricName)));
    when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.getMetricNamesFromMetricGroup(anyString(), anyString())).thenReturn(Flux.just(metricName));

    dataWriteService.ingest(Flux.fromIterable(List.of(
        Tuples.of(tenant, new Metric().setTimestamp(now).setValue(1.2).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(5)).setValue(1.5).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(10)).setValue(1.1).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(15)).setValue(3.4).setMetric(metricName).setTags(tags))
    ))).subscribe();

    final Long normalizedTimeSlot = DateTimeUtils.normalizedTimeslot(now, group);
    final PendingDownsampleSet pendingSet = new PendingDownsampleSet()
        .setSeriesSetHash(seriesSetHash)
        .setTenant(tenant)
        .setTimeSlot(Instant.ofEpochSecond(normalizedTimeSlot));
    DateTimeUtils.filterGroupGranularities(group, downsampleProperties.getGranularities()).forEach(granularity ->
        this.downsamplingService.downsampleData(
            pendingSet,
            granularity.getWidth(),
            queryService.fetchData(pendingSet, group, granularity.getWidth(), false)
        ).subscribe()
    );

    StepVerifier.create(queryService.queryDownsampled(tenant, "", metricGroup, Aggregator.min, Duration.ofMinutes(15), tags,
            now.minusSeconds(60 * 60), now.plusSeconds(24 * 60 * 60)).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getData().getTenant()).isEqualTo(tenant);
          assertThat(result.get(0).getData().getMetricName()).isEqualTo(metricName);
          assertThat(result.get(0).getData().getTags()).isEqualTo(tags);
        }).verifyComplete();
  }

  @Test
  void testTsdbQueryDownsampled() {
    final String tenant = randomAlphanumeric(10);
    final Instant now = Instant.now();
    final Instant start = now.minusSeconds(60 * 60);
    final Instant end = now.plusSeconds(24 * 60 * 60);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);

    final Map<String, String> tags = Map.of(
        "host", "h-1",
        "resource", "cpu-12",
        "metricGroup", metricGroup
    );

    final String seriesSetHash = seriesSetService.hash(metricName, tags);
    String group = "PT15M";
    List<Granularity> granularities = DateTimeUtils.filterGroupGranularities(group, downsampleProperties.getGranularities());

    when(ingestTrackingService.track(any(), anyString(), any())).thenReturn(Mono.empty());
    when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.updateDeviceAddMetricName(anyString(), anyString(), any(), any())).thenReturn(Mono.empty());
    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any())).thenReturn(Flux.just(seriesSetHash));
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(new MetricNameAndTags().setTags(tags).setMetricName(metricName)));
    when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());

    dataWriteService.ingest(Flux.fromIterable(List.of(
        Tuples.of(tenant, new Metric().setTimestamp(now).setValue(1.2).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(5)).setValue(1.5).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(10)).setValue(1.1).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(15)).setValue(3.4).setMetric(metricName).setTags(tags)),
        Tuples.of(tenant, new Metric().setTimestamp(now.plusSeconds(15 * 60)).setValue(8.4).setMetric(metricName).setTags(tags))
    ))).subscribe();

    TsdbQuery tsdbQuery = new TsdbQuery()
        .setSeriesSet(seriesSetHash)
        .setMetricName(metricName)
        .setTags(tags)
        .setGranularity(Duration.ofMinutes(15))
        .setAggregator(Aggregator.avg);

    final TsdbFilter filter = new TsdbFilter()
        .setType(FilterType.literal_or)
        .setTagk("host")
        .setFilter("h-1");

    final TsdbQueryRequest tsdbQueryRequest = new TsdbQueryRequest()
        .setMetric(metricName)
        .setDownsample("15m-avg")
        .setFilters(List.of(filter));

    when(metadataService.locateSeriesSetHashesFromQuery(any(), any())).thenReturn(Flux.just(tsdbQuery));
    when(metadataService.getTsdbQueries(List.of(tsdbQueryRequest), granularities)).thenReturn(Flux.just(tsdbQuery));

    final Instant normTS1 = Instant.ofEpochSecond(DateTimeUtils.normalizedTimeslot(now, group));
    final Instant normTS2 = Instant.ofEpochSecond(DateTimeUtils.normalizedTimeslot(now.plusSeconds(15 * 60), group));

    PendingDownsampleSet pendingSet = new PendingDownsampleSet()
        .setSeriesSetHash(seriesSetHash)
        .setTenant(tenant)
        .setTimeSlot(Instant.now());

    granularities.forEach(granularity ->
        List.of(normTS1, normTS2).forEach(timeslot ->
            this.downsamplingService.downsampleData(
                pendingSet.setTimeSlot(timeslot),
                granularity.getWidth(),
                queryService.fetchData(
                    pendingSet.setTimeSlot(timeslot),
                    group, granularity.getWidth(), false)
            ).subscribe()
        )
    );

    final Map<String, Double> expectedDps = Map.of(
        String.valueOf(normTS1.getEpochSecond()), 1.8,
        String.valueOf(normTS2.getEpochSecond()), 8.4
    );

    StepVerifier.create(
            queryService.queryTsdb(tenant, List.of(tsdbQueryRequest), start, end, granularities).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getMetric()).isEqualTo(metricName);
          assertThat(result.get(0).getTags()).isEqualTo(tags);
          assertThat(result.get(0).getAggregatedTags()).isEqualTo(Collections.emptyList());
          assertThat(result.get(0).getDps()).isEqualTo(expectedDps);
        }).verifyComplete();
  }
}
