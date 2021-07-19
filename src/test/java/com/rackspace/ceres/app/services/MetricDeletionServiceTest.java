package com.rackspace.ceres.app.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.Row;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.model.Metric;
import java.time.Duration;
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
  DownsampleProcessor downsampleProcessor;

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

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 4);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMetricsByTenantIdAndWithoutStartTime() {
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
        null, Instant.now()).then().block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 0);
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

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 0);
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

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 4);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
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

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 4);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMetricsByMetricNameAndWithoutStartTime() {
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
        null, Instant.now()).then().block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 0);
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

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 4);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMetricsByMetricNameAndTagAndWithoutStartTime() {
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
        null, Instant.now()).then().block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  /**
   * to test a scenario where in order to get series_set_hashes
   * we are querying data raw table first, if entries from data raw
   * has been expired, then it will its rolled up data table with
   * highest ttl
   */
  @Test
  public void testDeleteMetricsByTenantId_SeriesSetHash_From_Downsample_table() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final Number value = Math.random();
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
    //ingest data
    Metric metric = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(value)
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    //wait for it to be downsampled
    downsampleProcessor.downsampleData(
        Flux.just(
            singleValue(currentTime.toString(), value.doubleValue())
        ), tenantId, seriesSetHash,
        List.of(granularity(1, 12), granularity(2, 24)).iterator(),
        false
    ).block();

    //delete its entry from data raw table
    deleteDataFromRawTable(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime));

    metricDeletionService.deleteMetrics(tenantId, "", null,
        Instant.now().minus(5, ChronoUnit.MINUTES),
        Instant.now().plus(5, ChronoUnit.MINUTES)).then().block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 4);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  private void deleteDataFromRawTable(String tenant, Instant timeSlot) {
    cqlTemplate.queryForRows(dataTablesStatements.getRawDelete(), tenant, timeSlot)
        .collectList().block();
  }

  private Granularity granularity(int minutes, int ttlHours) {
    return new Granularity()
        .setWidth(Duration.ofMinutes(minutes))
        .setTtl(Duration.ofHours(ttlHours));
  }

  private ValueSet singleValue(String timestamp, double value) {
    return new SingleValueSet()
        .setValue(value).setTimestamp(Instant.parse(timestamp));
  }

  private void assertMetricNamesViaQuery(String tenant, String metricName, int expectedRowNum)  {
    final List<Row> metricNamesResult = cqlTemplate.queryForRows(QUERY_METRIC_NAMES,
        tenant, metricName
    ).collectList().block();
    assertThat(metricNamesResult).hasSize(expectedRowNum);
  }

  private void assertSeriesSetViaQuery(String tenant, String metricName, int expectedRowNum)  {
    final List<Row> seriesSetsResult = cqlTemplate.queryForRows(QUERY_SERIES_SET, tenant,
        metricName).collectList().block();
    assertThat(seriesSetsResult).hasSize(expectedRowNum);
  }

  private void assertSeriesSetHashesViaQuery(String tenant, String seriesSetHash,
      int expectedRowNum)  {
    final List<Row> seriesSetHashesResult = cqlTemplate.queryForRows(QUERY_SERIES_SET_HASHES,
        tenant, seriesSetHash
    ).collectList().block();
    assertThat(seriesSetHashesResult).hasSize(expectedRowNum);
  }

  private void assertQueryRawViaQuery(String tenant, Instant timeSlot, int expectedRowNum) {
    final List<Row> queryRawResult = cqlTemplate.queryForRows(QUERY_RAW, tenant, timeSlot)
        .collectList().block();

    assertThat(queryRawResult).hasSize(expectedRowNum);
  }


}
