package com.rackspace.ceres.app.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.Row;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
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
  DataWriteService dataWriteService;
  @Autowired
  SeriesSetService seriesSetService;
  @Autowired
  TimeSlotPartitioner timeSlotPartitioner;
  @Autowired
  DownsampleProcessor downsampleProcessor;
  @Autowired
  DownsamplingService downsamplingService;

  @Autowired
  MetadataService metadataService;
  @Autowired
  DataTablesStatements dataTablesStatements;
  @MockBean
  IngestTrackingService ingestTrackingService;

  @MockBean
  QueryService queryService;

  @MockBean
  ElasticSearchService elasticSearchService;

  private static final String QUERY_RAW = "SELECT * FROM data_raw_p_pt1h WHERE tenant = ?"
      + " AND time_slot = ?";
  private static final String QUERY_SERIES_SET_HASHES = "SELECT * FROM series_set_hashes"
      + " WHERE tenant = ? AND series_set_hash = ?";
  private static final String QUERY_SERIES_SET = "SELECT * FROM series_sets"
      + " WHERE tenant = ? AND metric_name = ?";
  private static final String QUERY_METRIC_NAMES = "SELECT * FROM metric_names"
      + " WHERE tenant = ? AND metric_name = ?";
  private static final String QUERY_METRIC_GROUPS = "SELECT * FROM metric_groups"
      + " WHERE tenant = ? AND metric_group = ?";
  private static final String QUERY_DEVICES = "SELECT * FROM devices"
      + " WHERE tenant = ? AND device = ?";
  private static final String QUERY_TAGS_DATA = "SELECT * from tags_data where tenant = ? AND type IN ('TAGK', 'TAGV')";

  @Test
  public void testDeleteMetricsByTenantId() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService
        .deleteMetricsByTenantId(tenantId, Instant.now().minusSeconds(60), Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);

    //validate tags_data
    assertTagsDataViaQuery(tenantId, 12);
  }

  @Test
  public void testDeleteMetricsByTenantIdAndWithoutStartTime() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetricsByTenantId(tenantId, null, Instant.now()).block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 0);

    //validate tags_data
    assertTagsDataViaQuery(tenantId, 0);
  }

  @Test
  public void testDeleteMetricsByTenantId_No_Data() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();

    metricDeletionService
        .deleteMetricsByTenantId(tenantId, Instant.now().minusSeconds(60), Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 0);

    //validate tags_data
    assertTagsDataViaQuery(tenantId, 0);
  }

  @Test
  public void testDeleteMetricsByTenantId_Invalid_Time_Range() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
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

    metricDeletionService.deleteMetricsByTenantId(tenantId, Instant.now(), Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);

    //validate tags_data
    assertTagsDataViaQuery(tenantId, 12);
  }

  @Test
  public void testDeleteMetricsByMetricName() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
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

    metricDeletionService
        .deleteMetricsByMetricName(tenantId, metricName, Instant.now().minusSeconds(60),
            Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime), 0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMetricsByMetricNameAndWithoutStartTime() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
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

    metricDeletionService
        .deleteMetricsByMetricName(tenantId, metricName, null, Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
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

    metricDeletionService
        .deleteMetricsByMetricNameAndTag(tenantId, metricName, List.of("host=h-1"),
            Instant.now().minusSeconds(60), Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMetricsByMetricNameAndTagAndWithoutStartTime() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
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

    metricDeletionService
        .deleteMetricsByMetricNameAndTag(tenantId, metricName, List.of("host=h-1"),
            null, Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

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
    final String resource = RandomStringUtils.randomAlphabetic(5);
    final String monitoring_system = RandomStringUtils.randomAlphanumeric(5);
    final Number value = Math.random();
    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "metricGroup", metricGroup,
        "resource", resource,
        "monitoring_system", monitoring_system
    );
    final String seriesSetHash = seriesSetService.hash(metricName, tags);

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    String group = "PT15M";
    final Long normalizedTimeSlot = DateTimeUtils.normalizedTimeslot(currentTime, group);

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(value)
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Flux<ValueSet> data = Flux.fromIterable(List.of(singleValue(currentTime.toString(), value.doubleValue())));

    final PendingDownsampleSet pendingSet = new PendingDownsampleSet()
        .setTenant(tenantId)
        .setSeriesSetHash(seriesSetHash)
        .setTimeSlot(Instant.ofEpochSecond(normalizedTimeSlot));

    List.of(granularity(1, 12), granularity(2, 24)).forEach(granularity ->
        this.downsamplingService.downsampleData(pendingSet, granularity.getWidth(), data).subscribe()
    );

    //delete its entry from data raw table
    deleteDataFromRawTable(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime));

    metricDeletionService
        .deleteMetricsByTenantId(tenantId, Instant.now().minus(5, ChronoUnit.MINUTES),
            Instant.now().plus(5, ChronoUnit.MINUTES))
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMultipleMetricsByTenantIdAndWithoutStartTime() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime.minus(10, ChronoUnit.SECONDS))
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime.plus(10, ChronoUnit.SECONDS))
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        3);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);

    //validate tags_data
    assertTagsDataViaQuery(tenantId, 12);

    metricDeletionService.deleteMetricsByTenantId(tenantId, null, Instant.now()).block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 0);

    //validate tags_data
    assertTagsDataViaQuery(tenantId, 0);
  }

  @Test
  public void testDeleteByTenantIdAndWithoutStartTimeByCheckingBoundaryConditions1() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTimeMinus5Hours = Instant.now().minus(5, ChronoUnit.HOURS);
    Metric metric1 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus5Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus4Hours = Instant.now().minus(4, ChronoUnit.HOURS);
    Metric metric2 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus4Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus3Hours = Instant.now().minus(3, ChronoUnit.HOURS);
    Metric metric3 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus3Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus2Hours = Instant.now().minus(2, ChronoUnit.HOURS);
    Metric metric4 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus2Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus1Hours = Instant.now().minus(1, ChronoUnit.HOURS);
    Metric metric5 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus1Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus5Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus4Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus3Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus2Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus1Hours),
        1);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);

    metricDeletionService
        .deleteMetricsByTenantId(tenantId, Instant.now().minus(3, ChronoUnit.HOURS), Instant.now())
        .block();

    //validate data raw
    //it must not delete metrics which are older than 3 hours
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus5Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus4Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus3Hours),
        0);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus2Hours),
        0);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus1Hours),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteByTenantIdAndWithoutStartTimeByCheckingBoundaryConditions2() {
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTimeMinus5Hours = Instant.now().minus(5, ChronoUnit.HOURS);
    Metric metric1 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus5Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus4Hours = Instant.now().minus(4, ChronoUnit.HOURS);
    Metric metric2 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus4Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus3Hours = Instant.now().minus(3, ChronoUnit.HOURS);
    Metric metric3 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus3Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus2Hours = Instant.now().minus(2, ChronoUnit.HOURS);
    Metric metric4 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus2Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    Instant currentTimeMinus1Hours = Instant.now().minus(1, ChronoUnit.HOURS);
    Metric metric5 = dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTimeMinus1Hours)
            .setValue(Math.random())
            .setMetric(metricName)
            .setTags(tags)
    ).block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus5Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus4Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus3Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus2Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus1Hours),
        1);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);

    metricDeletionService
        .deleteMetricsByTenantId(tenantId, Instant.now().minus(4, ChronoUnit.HOURS),
            Instant.now().minus(2, ChronoUnit.HOURS))
        .block();

    //validate data raw
    //it must not delete metrics which are in range of 4 to 2 hours back from current time
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus5Hours),
        1);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus4Hours),
        0);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus3Hours),
        0);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus2Hours),
        0);
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTimeMinus1Hours),
        1);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName, 1);
  }

  @Test
  public void testDeleteMetricsByMetricGroup_WithStartTime() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName1)
            .setTags(tags)
    ).block();

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName2)
            .setTags(tags)
    ).block();

    metricDeletionService
        .deleteMetricsByMetricGroup(tenantId, metricGroup, Instant.now().minusSeconds(60),
            Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash1, 1);
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash2, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName1, 6);
    assertSeriesSetViaQuery(tenantId, metricName2, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName1, 1);
    assertMetricNamesViaQuery(tenantId, metricName2, 1);

    //validate metric_groups
    assertMetricGroupViaQuery(tenantId, metricGroup, 1);

    //validate devices
    assertDevicesViaQuery(tenantId, resource, 1);
  }

  @Test
  public void testDeleteMetricsByMetricGroup_WithoutStartTime() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
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

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName1)
            .setTags(tags)
    ).block();

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName2)
            .setTags(tags)
    ).block();

    metricDeletionService.deleteMetricsByMetricGroup(tenantId, metricGroup, null, Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        0);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash1, 0);
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash2, 0);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName1, 0);
    assertSeriesSetViaQuery(tenantId, metricName2, 0);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName1, 0);
    assertMetricNamesViaQuery(tenantId, metricName2, 0);

    //validate metric_groups
    assertMetricGroupViaQuery(tenantId, metricGroup, 0);
  }

  @Test
  public void testPartialDeletionOfMetricNamesFromMetricgroupsAndDevices() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName1 = RandomStringUtils.randomAlphabetic(5);
    final String metricName2 = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final String resource = RandomStringUtils.randomAlphabetic(5);
    final String monitoring_system = RandomStringUtils.randomAlphanumeric(5);
    final Map<String, String> tags1 = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod",
        "metricGroup", metricGroup,
        "resource", resource,
        "monitoring_system", monitoring_system
    );

    final String seriesSetHash1 = seriesSetService.hash(metricName1, tags1);
    final String seriesSetHash2 = seriesSetService.hash(metricName2, tags1);

    when(ingestTrackingService.track(any(), anyString(), any()))
        .thenReturn(Mono.empty());
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    Instant currentTime = Instant.now();
    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName1)
            .setTags(tags1)
    ).block();

    dataWriteService.ingest(
        tenantId,
        new Metric()
            .setTimestamp(currentTime)
            .setValue(Math.random())
            .setMetric(metricName2)
            .setTags(tags1)
    ).block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        2);

    metricDeletionService.deleteMetricsByMetricName(tenantId, metricName1, null, Instant.now())
        .block();

    //validate data raw
    assertQueryRawViaQuery(tenantId, timeSlotPartitioner.rawTimeSlot(currentTime),
        1);

    //validate series_set_hashes
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash1, 0);
    assertSeriesSetHashesViaQuery(tenantId, seriesSetHash2, 1);

    //validate series_sets
    assertSeriesSetViaQuery(tenantId, metricName1, 0);
    assertSeriesSetViaQuery(tenantId, metricName2, 6);

    //validate metric_names
    assertMetricNamesViaQuery(tenantId, metricName1, 0);
    assertMetricNamesViaQuery(tenantId, metricName2, 1);

    //validate metric_groups
    assertMetricGroupViaQuery(tenantId, metricGroup, 1);

    //validate devices
    assertDevicesViaQuery(tenantId, resource, 1);
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

  private void assertMetricGroupViaQuery(String tenant, String metricGroup, int expectedRowNum)  {
    final List<Row> metricNamesResult = cqlTemplate.queryForRows(QUERY_METRIC_GROUPS,
        tenant, metricGroup
    ).collectList().block();
    assertThat(metricNamesResult).hasSize(expectedRowNum);
  }

  private void assertDevicesViaQuery(String tenant, String device, int expectedRowNum)  {
    final List<Row> devicesResult = cqlTemplate.queryForRows(QUERY_DEVICES,
        tenant, device
    ).collectList().block();
    assertThat(devicesResult).hasSize(expectedRowNum);
  }

  private void assertQueryRawViaQuery(String tenant, Instant timeSlot, int expectedRowNum) {
    final List<Row> queryRawResult = cqlTemplate.queryForRows(QUERY_RAW, tenant, timeSlot)
        .collectList().block();

    assertThat(queryRawResult).hasSize(expectedRowNum);
  }

  private void assertTagsDataViaQuery(String tenant, int expectedRowNum) {
    final List<Row> tagsDataResult = cqlTemplate.queryForRows(QUERY_TAGS_DATA, tenant)
        .collectList().block();
    assertThat(tagsDataResult).hasSize(expectedRowNum);
  }
}
