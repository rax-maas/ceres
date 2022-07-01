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
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import com.rackspace.ceres.app.model.TsdbQuery;
import com.rackspace.ceres.app.model.TsdbQueryRequest;
import com.rackspace.ceres.app.model.TsdbFilter;
import com.rackspace.ceres.app.model.FilterType;

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

@SpringBootTest
@ActiveProfiles(profiles = {"test", "downsample"})
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
  DownsampleProperties downsampleProperties;

  @AfterEach
  void tearDown() {
    cassandraTemplate.truncate(MetricName.class)
        .and(cassandraTemplate.truncate(SeriesSet.class))
        .block();
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

    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(Instant.now())
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    StepVerifier.create(queryService
        .queryRaw(tenantId, metricName,"", tags, Instant.now().minusSeconds(60), Instant.now()).collectList())
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
    Metric metric = dataWriteService.ingest(
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
        "metricGroup", metricGroup
    );

    final String seriesSetHash = seriesSetService
        .hash(metricName, tags);

    when(metadataService.storeMetadata(any(), any(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any()))
        .thenReturn(Flux.just(seriesSetHash));

    MetricNameAndTags metricNameAndTags = new MetricNameAndTags().setTags(tags).setMetricName(metricName);
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(metricNameAndTags));
    when(metadataService.updateMetricGroupAddMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());

    dataWriteService.ingest(
        tenant,
        new Metric()
            .setTimestamp(Instant.now())
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).subscribe();

    downsampleProcessor.downsampleData(
        Flux.just(
            singleValue(Instant.now().toString(), 1.2),
            singleValue(Instant.now().plusSeconds(5).toString(), 1.5),
            singleValue(Instant.now().plusSeconds(10).toString(), 1.1),
            singleValue(Instant.now().plusSeconds(15).toString(), 3.4)
        ), tenant, seriesSetHash,
        List.of(granularity(1, 12), granularity(2, 24)).iterator()
    ).block();

    StepVerifier.create(queryService.queryDownsampled(tenant, metricName, "", Aggregator.min, Duration.ofMinutes(2), tags,
        Instant.now().minusSeconds(5*60), Instant.now().plusSeconds(24*60*60)).collectList())
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
    when(metadataService.getMetricNamesFromMetricGroup(anyString(), anyString())).thenReturn(Flux.just(metricName));

    dataWriteService.ingest(
        tenant,
        new Metric()
            .setTimestamp(Instant.now())
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    downsampleProcessor.downsampleData(
        Flux.just(
            singleValue(Instant.now().toString(), 1.2),
            singleValue(Instant.now().plusSeconds(5).toString(), 1.5),
            singleValue(Instant.now().plusSeconds(10).toString(), 1.1),
            singleValue(Instant.now().plusSeconds(15).toString(), 3.4)
        ), tenant, seriesSetHash,
        List.of(granularity(1, 12), granularity(2, 24)).iterator()
    ).block();

    StepVerifier.create(queryService.queryDownsampled(tenant, "", metricGroup, Aggregator.min, Duration.ofMinutes(2), tags,
        Instant.now().minusSeconds(5*60), Instant.now().plusSeconds(24*60*60)).collectList())
        .assertNext(result -> {
          assertThat(result).isNotEmpty();
          assertThat(result.get(0).getData().getTenant()).isEqualTo(tenant);
          assertThat(result.get(0).getData().getMetricName()).isEqualTo(metricName);
          assertThat(result.get(0).getData().getTags()).isEqualTo(tags);
        }).verifyComplete();
  }

  @Test
  void testTsdbQueryDownsampled() {
    final long epochSecondsNow = 1616686440L;
    final long epochSecondsNowPlus2Min = 1616686440 + 120;
    final String tenant = randomAlphanumeric(10);
    final Instant now = Instant.ofEpochSecond(epochSecondsNow);
    final Instant start = now.minusSeconds(5 * 60);
    final Instant end = now.plusSeconds(24 * 60 * 60);
    final String metricName = RandomStringUtils.randomAlphabetic(5);

    final Map<String, String> tags = Map.of(
            "host", "h-1"
    );

    final String seriesSetHash = seriesSetService.hash(metricName, tags);

    TsdbQuery tsdbQuery = new TsdbQuery()
            .setSeriesSet(seriesSetHash)
            .setMetricName(metricName)
            .setTags(tags)
            .setGranularity(Duration.ofMinutes(2))
            .setAggregator(Aggregator.avg);

    List<Granularity> granularities = List.of(granularity(1, 12), granularity(2, 24));

    final TsdbFilter filter = new TsdbFilter()
            .setType(FilterType.literal_or)
            .setTagk("host")
            .setFilter("h-1");

    final TsdbQueryRequest tsdbQueryRequest = new TsdbQueryRequest()
            .setMetric(metricName)
            .setDownsample("2m-avg")
            .setFilters(List.of(filter));

    when(metadataService.locateSeriesSetHashesFromQuery(any(), any())).thenReturn(Flux.just(tsdbQuery));
    when(metadataService.getTsdbQueries(
            List.of(tsdbQueryRequest), granularities)).thenReturn(Flux.just(tsdbQuery));

    downsampleProcessor.downsampleData(
            Flux.just(
                    singleValue(now.toString(), 1.2),
                    singleValue(now.plusSeconds(5).toString(), 1.5),
                    singleValue(now.plusSeconds(10).toString(), 1.1),
                    singleValue(now.plusSeconds(15).toString(), 3.4),
                    singleValue(now.plusSeconds(200).toString(), 8.4)
            ), tenant, seriesSetHash,
            granularities.iterator()
    ).subscribe();

    final Map<String, Double> expectedDps = Map.of(
        Long.toString(epochSecondsNow), 1.8,
        Long.toString(epochSecondsNowPlus2Min), 8.4
    );

    StepVerifier.create(
            queryService.queryTsdb(tenant, List.of(tsdbQueryRequest), start, end, granularities).collectList())
            .assertNext(result -> {
              log.info("result: {}", result);
              assertThat(result).isNotEmpty();
              assertThat(result.get(0).getMetric()).isEqualTo(metricName);
              assertThat(result.get(0).getTags()).isEqualTo(tags);
              assertThat(result.get(0).getAggregatedTags()).isEqualTo(Collections.emptyList());
              assertThat(result.get(0).getDps()).isEqualTo(expectedDps);
            }).verifyComplete();
  }

  private ValueSet singleValue(String timestamp, double value) {
    return new SingleValueSet()
        .setValue(value).setTimestamp(Instant.parse(timestamp));
  }

  private Granularity granularity(int minutes, int ttlHours) {
    return new Granularity()
        .setWidth(Duration.ofMinutes(minutes))
        .setTtl(Duration.ofHours(ttlHours));
  }
}
