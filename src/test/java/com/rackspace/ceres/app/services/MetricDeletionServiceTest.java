package com.rackspace.ceres.app.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.Row;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

@ActiveProfiles(profiles = {"downsample", "test"})
@EnableConfigurationProperties(DownsampleProperties.class)
@SpringBootTest
@Testcontainers
public class MetricDeletionServiceTest {

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

  @Autowired
  MetricDeletionService metricDeletionService;
  @Autowired
  ReactiveCqlTemplate cqlTemplate;
  @Autowired
  DownsampleProperties downsampleProperties;
  @Autowired
  DataWriteService dataWriteService;
  @MockBean
  MetadataService metadataService;
  @Autowired
  SeriesSetService seriesSetService;
  @MockBean
  DownsampleTrackingService downsampleTrackingService;

  @Test
  public void testDeleteMetricsByTenantId() {
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

    when(downsampleTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.storeMetadata(any(), any(), any(), any()))
        .thenReturn(Mono.empty());

    when(metadataService.locateSeriesSetHashes(anyString(), anyString(), any()))
        .thenReturn(Flux.just(seriesSetHash));

    MetricNameAndTags metricNameAndTags = new MetricNameAndTags().setTags(tags).setMetricName(metricName);
    when(metadataService.resolveSeriesSetHash(anyString(), anyString()))
        .thenReturn(Mono.just(metricNameAndTags));

    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(Instant.now())
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetrics(tenantId, "", null,
        Instant.now().minusSeconds(60), Instant.now()).then().block();

    assertViaQuery(tenantId, Instant.parse("2020-09-12T18:00:00.0Z"), seriesSetHash, metric);
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
