package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricNameAndTags;
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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles(profiles = {"test", "downsample", "query"})
@Testcontainers
@Slf4j
class DownsampleQueryServiceTest {

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
  DownsampleQueryService queryService;

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
}

