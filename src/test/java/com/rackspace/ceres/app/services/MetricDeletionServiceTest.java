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
import java.time.temporal.ChronoUnit;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

  static {
    GenericContainer redis = new GenericContainer("redis:3-alpine")
        .withExposedPorts(6379);
    redis.start();

    System.setProperty("spring.redis.host", redis.getContainerIpAddress());
    System.setProperty("spring.redis.port", redis.getFirstMappedPort() + "");
  }

  @Autowired
  MetricDeletionService metricDeletionService;
  @Autowired
  ReactiveCqlTemplate cqlTemplate;
  @Autowired
  DownsampleProperties downsampleProperties;
  @Autowired
  DataWriteService dataWriteService;
  @Autowired
  SeriesSetService seriesSetService;
  @Autowired
  TimeSlotPartitioner timeSlotPartitioner;

  @Autowired
  MetadataService metadataService;
  @Autowired
  DataTablesStatements dataTablesStatements;
  @MockBean
  DownsampleTrackingService downsampleTrackingService;

  private static final String QUERY_RAW = "SELECT * FROM data_raw_p_pt1h WHERE tenant = ?"
      + " AND time_slot = ?";
  private static final String QUERY_SERIES_SET_HASHES = "SELECT * FROM series_set_hashes"
      + " WHERE tenant = ? AND series_set_hash = ?";
  private static final String QUERY_SERIES_SET = "SELECT * FROM series_sets"
      + " WHERE tenant = ? AND metric_name = ?";
  private static final String QUERY_METRIC_NAMES = "SELECT * FROM metric_names"
      + " WHERE tenant = ? AND metric_name = ?";

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

    Instant currentTime = Instant.now();
    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetrics(tenantId, "", null,
        Instant.now().minusSeconds(60), Instant.now()).then().block();

    assertViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), seriesSetHash,
        metricName);

    //validate metric_names
    final List<Row> metricNamesResult = cqlTemplate.queryForRows(QUERY_METRIC_NAMES,
        tenantId, metricName
    ).collectList().block();
    assertThat(metricNamesResult).hasSize(0);
  }

  @Test
  public void testDeleteMetricsByTenantId_No_Data() {
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

    Instant currentTime = Instant.now();

    metricDeletionService.deleteMetrics(tenantId, "", null,
        Instant.now().minusSeconds(60), Instant.now()).then().block();

    assertViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), seriesSetHash, metricName);
  }

  @Test
  public void testDeleteMetricsByTenantId_Invalid_Time_Range() {
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

    Instant currentTime = Instant.now();
    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetrics(tenantId, "", null,
        Instant.now(), Instant.now()).then().block();

    assertViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), seriesSetHash, metricName);
  }

  @Test
  public void testDeleteMetricsByMetricName() {
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

    Instant currentTime = Instant.now();
    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetrics(tenantId, metricName, null,
        Instant.now().minusSeconds(60), Instant.now()).then().block();

    assertViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), seriesSetHash, metricName);

    //validate metric_names
    final List<Row> metricNamesResult = cqlTemplate.queryForRows(
        "SELECT * FROM metric_names"
            + " WHERE tenant = ?"
            + " AND metric_name = ?",
        tenantId, metricName
    ).collectList().block();
    assertThat(metricNamesResult).hasSize(0);
  }

  @Test
  public void testDeleteMetricsByMetricNameAndTag() {
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

    Instant currentTime = Instant.now();
    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetrics(tenantId, metricName, List.of("host=h-1"),
        Instant.now().minusSeconds(60), Instant.now()).then().block();

    assertViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), seriesSetHash, metricName);
  }

  private void assertViaQuery(String tenant, Instant timeSlot, String seriesSetHash,
      String metricName) {
    //validate data raw
    final List<Row> queryRawResult = cqlTemplate.queryForRows(QUERY_RAW, tenant, timeSlot)
        .collectList().block();

    assertThat(queryRawResult).hasSize(0);

    //validate series_set_hashes
    final List<Row> seriesSetHashesResult = cqlTemplate.queryForRows(QUERY_SERIES_SET_HASHES,
        tenant, seriesSetHash
    ).collectList().block();

    assertThat(seriesSetHashesResult).hasSize(0);

    //validate series_sets
    final List<Row> seriesSetsResult = cqlTemplate.queryForRows(QUERY_SERIES_SET, tenant,
        metricName).collectList().block();

    assertThat(seriesSetsResult).hasSize(0);
  }
}
