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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.Row;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.model.Metric;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
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
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

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
  DownsampleTrackingService downsampleTrackingService;

  @Autowired
  SeriesSetService seriesSetService;

  @Autowired
  DataWriteService dataWriteService;

  @Autowired
  ReactiveCqlTemplate cqlTemplate;

  @Nested
  @NestedTestConfiguration(value = EnclosingConfiguration.OVERRIDE)
  class ingest {
    @Test
    void testSingle() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphabetic(5);
      final String metricGroup = RandomStringUtils.randomAlphabetic(5);
      final Map<String, String> tags = Map.of(
          "os", "linux",
          "host", "h-1",
          "deployment", "prod",
          "metricGroup", metricGroup
      );
      final String seriesSetHash = seriesSetService.hash(metricName, tags);

      when(metadataService.storeMetadata(any(), any(), any(), any()))
          .thenReturn(Mono.empty());

      when(downsampleTrackingService.track(any(), anyString(), any()))
          .thenReturn(Mono.empty());

      final Metric metric = dataWriteService.ingest(
          tenantId,
          new Metric()
              .setTimestamp(Instant.parse("2020-09-12T18:42:23.658447900Z"))
              .setValue(Math.random())
              .setMetric(metricName)
              .setTags(tags)
      )
          .block();

      assertThat(metric).isNotNull();

      assertViaQuery(tenantId, Instant.parse("2020-09-12T18:00:00.0Z"), seriesSetHash, metric);

      verify(metadataService).storeMetadata(tenantId, seriesSetHash, metric.getMetric(),
          metric.getTags());

      verify(downsampleTrackingService).track(tenantId, seriesSetHash, metric.getTimestamp());

      verifyNoMoreInteractions(metadataService, downsampleTrackingService);
    }

    @Test
    void testMulti() {
      final String tenant1 = RandomStringUtils.randomAlphanumeric(10);
      final String tenant2 = RandomStringUtils.randomAlphanumeric(10);
      final String metricName1 = RandomStringUtils.randomAlphabetic(5);
      final String metricName2 = RandomStringUtils.randomAlphabetic(5);
      final String metricGroup = RandomStringUtils.randomAlphabetic(5);
      final Map<String, String> tags = Map.of(
          "os", "linux",
          "host", "h-1",
          "deployment", "prod",
          "metricGroup", metricGroup
      );
      final String seriesSetHash1 = seriesSetService.hash(metricName1, tags);
      final String seriesSetHash2 = seriesSetService.hash(metricName2, tags);

      when(metadataService.storeMetadata(any(), any(), any(), any()))
          .thenReturn(Mono.empty());

      when(downsampleTrackingService.track(any(), anyString(), any()))
          .thenReturn(Mono.empty());

      final Metric metric1 = new Metric()
          .setTimestamp(Instant.parse("2020-09-12T18:42:23.658447900Z"))
          .setValue(Math.random())
          .setMetric(metricName1)
          .setTags(tags);
      final Metric metric2 = new Metric()
          .setTimestamp(Instant.parse("2020-09-12T18:42:23.658447900Z"))
          .setValue(Math.random())
          .setMetric(metricName2)
          .setTags(tags);

      dataWriteService.ingest(Flux.just(
          Tuples.of(tenant1, metric1),
          Tuples.of(tenant2, metric2)
      )).then().block();

      assertViaQuery(tenant1, Instant.parse("2020-09-12T18:00:00.0Z"), seriesSetHash1, metric1);
      assertViaQuery(tenant2, Instant.parse("2020-09-12T18:00:00.0Z"), seriesSetHash2, metric2);

      verify(metadataService).storeMetadata(tenant1, seriesSetHash1, metric1.getMetric(),
          metric1.getTags());
      verify(metadataService).storeMetadata(tenant2, seriesSetHash2, metric2.getMetric(),
          metric2.getTags());

      verify(downsampleTrackingService).track(tenant1, seriesSetHash1, metric1.getTimestamp());
      verify(downsampleTrackingService).track(tenant2, seriesSetHash2, metric2.getTimestamp());

      verifyNoMoreInteractions(metadataService, downsampleTrackingService);
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
      assertThat(results.get(0).getInstant(0)).isEqualTo("2020-09-12T18:42:23.658Z");
      assertThat(results.get(0).getDouble(1)).isEqualTo(metric.getValue());
    }

  }

}
