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

import com.datastax.oss.driver.api.core.cql.Row;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.Metric;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.NestedTestConfiguration;
import org.springframework.test.context.NestedTestConfiguration.EnclosingConfiguration;
import org.springframework.web.server.ServerWebInputException;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class DataWriteServiceTest {

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

    @Bean
    MeterRegistry meterRegistry() {
      return new SimpleMeterRegistry();
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

  @MockBean
  IngestTrackingService ingestTrackingService;

  @Autowired
  SeriesSetService seriesSetService;

  @Autowired
  DataWriteService dataWriteService;

  @Autowired
  ReactiveCqlTemplate cqlTemplate;

  @Autowired
  MeterRegistry meterRegistry;

  @AfterEach
  void resetMeterRegistry()  {
    meterRegistry.clear();
  }

  @Nested
  @NestedTestConfiguration(value = EnclosingConfiguration.OVERRIDE)
  class ingest {

    @Test
    void testSingle() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
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
      final String seriesSetHash = seriesSetService.hash(metricName, tags);

      when(metadataService.storeMetadata(any(), any(), any())).thenReturn(Mono.empty());

      when(ingestTrackingService.track(any(), anyString(), any()))
          .thenReturn(Mono.empty());

      Instant metricTime = validMetricTime();
      final Metric metric = dataWriteService.ingest(
          tenantId,
          new Metric()
              .setTimestamp(metricTime)
              .setValue(Math.random())
              .setMetric(metricName)
              .setTags(tags)
      )
          .block();

      assertThat(metric).isNotNull();
      Instant metricQueryTime = metricTime.with(new TemporalNormalizer(Duration.ofHours(1)));
      assertViaQuery(tenantId, metricQueryTime, seriesSetHash, metric);

      verify(metadataService).storeMetadata(tenantId, seriesSetHash, metric);
      verify(ingestTrackingService).track(tenantId, seriesSetHash, metric.getTimestamp());
      verifyNoMoreInteractions(metadataService, ingestTrackingService);
    }

    @Test
    void testMulti() {
      final String tenant1 = RandomStringUtils.randomAlphanumeric(10);
      final String tenant2 = RandomStringUtils.randomAlphanumeric(10);
      final String metricName1 = RandomStringUtils.randomAlphabetic(5);
      final String metricName2 = RandomStringUtils.randomAlphabetic(5);
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
      final String seriesSetHash1 = seriesSetService.hash(metricName1, tags);
      final String seriesSetHash2 = seriesSetService.hash(metricName2, tags);

      when(metadataService.storeMetadata(any(), any(), any()))
          .thenReturn(Mono.empty());

      when(ingestTrackingService.track(any(), anyString(), any()))
          .thenReturn(Mono.empty());

      Instant metricTime = validMetricTime();
      final Metric metric1 = new Metric()
          .setTimestamp(metricTime)
          .setValue(Math.random())
          .setMetric(metricName1)
          .setTags(tags);
      final Metric metric2 = new Metric()
          .setTimestamp(metricTime)
          .setValue(Math.random())
          .setMetric(metricName2)
          .setTags(tags);

      dataWriteService.ingest(Flux.just(
          Tuples.of(tenant1, metric1),
          Tuples.of(tenant2, metric2)
      )).then().block();

      Instant metricQueryTime = metricTime.with(new TemporalNormalizer(Duration.ofHours(1)));
      assertViaQuery(tenant1, metricQueryTime, seriesSetHash1, metric1);
      assertViaQuery(tenant2, metricQueryTime, seriesSetHash2, metric2);

      verify(metadataService).storeMetadata(tenant1, seriesSetHash1, metric1);
      verify(metadataService).storeMetadata(tenant2, seriesSetHash2, metric2);

      verify(ingestTrackingService).track(tenant1, seriesSetHash1, metric1.getTimestamp());
      verify(ingestTrackingService).track(tenant2, seriesSetHash2, metric2.getTimestamp());

      verifyNoMoreInteractions(metadataService, ingestTrackingService);
    }

    @Test
    void testInvalidOldMetric() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphabetic(5);
      final Map<String, String> tags = Map.of(
              "os", "linux",
              "host", "h-1",
              "deployment", "prod",
              "metricGroup", "group",
              "resource", RandomStringUtils.randomAlphabetic(5)
      );

      Mono<?> result = dataWriteService.ingest(
              tenantId,
              new Metric()
                      .setTimestamp(Instant.now().minus(8, ChronoUnit.DAYS))
                      .setValue(Math.random())
                      .setMetric(metricName)
                      .setTags(tags));

      StepVerifier.create(result).expectErrorMatches(throwable -> throwable instanceof ServerWebInputException &&
              throwable.getMessage().equals("400 BAD_REQUEST \"Metric timestamp is out of bounds\"")).verify();
    }

    @Test
    void testInvalidFutureMetric() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphabetic(5);
      final Map<String, String> tags = Map.of(
              "os", "linux",
              "host", "h-1",
              "deployment", "prod",
              "metricGroup", "group",
              "resource", RandomStringUtils.randomAlphabetic(5)
      );

      Mono<?> result = dataWriteService.ingest(
              tenantId,
              new Metric()
                      .setTimestamp(Instant.now().plus(2, ChronoUnit.DAYS))
                      .setValue(Math.random())
                      .setMetric(metricName)
                      .setTags(tags));

      StepVerifier.create(result).expectErrorMatches(throwable -> throwable instanceof ServerWebInputException &&
              throwable.getMessage().equals("400 BAD_REQUEST \"Metric timestamp is out of bounds\"")).verify();
    }

    private Instant validMetricTime() {
      // Random Metric within valid bounds of ingestion time
      return Instant.now().atZone(ZoneOffset.UTC).withHour(18).withMinute(42).withSecond(23).toInstant();
    }

    private void assertViaQuery(String tenant, Instant timeSlot, String seriesSetHash,
                                Metric metric) {
      final List<Row> results = cqlTemplate.queryForRows(
          "SELECT ts, value FROM data_raw_p_pt1h"
              + " WHERE tenant = ?"
              + " AND time_slot = ?"
              + " AND series_set_hash = ?",
          tenant, timeSlot, seriesSetHash
      ).collectList().block();

      assertThat(results).isNotNull();
      assertThat(results).hasSize(1);
      // only millisecond resolution retained by cassandra
      assertThat(results.get(0).getInstant(0)).isEqualTo(metric.getTimestamp().truncatedTo(ChronoUnit.MILLIS));
      assertThat(results.get(0).getDouble(1)).isEqualTo(metric.getValue());
    }

  }

}
