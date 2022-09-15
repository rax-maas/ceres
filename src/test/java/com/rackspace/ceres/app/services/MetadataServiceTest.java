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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.ESContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.model.Criteria;
import com.rackspace.ceres.app.model.FilterType;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricDTO;
import com.rackspace.ceres.app.model.MetricNameAndMultiTags;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import com.rackspace.ceres.app.model.TsdbFilter;
import com.rackspace.ceres.app.model.TsdbQuery;
import com.rackspace.ceres.app.model.TsdbQueryRequest;
import com.rackspace.ceres.app.services.MetadataServiceTest.RedisEnvInit;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootTest
@ActiveProfiles(profiles = {"test", "query"})
@ContextConfiguration(initializers = RedisEnvInit.class)
@Testcontainers
@Slf4j
class MetadataServiceTest {
  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>(
      CassandraContainerSetup.DOCKER_IMAGE);

  @Container
  public static ESContainerSetup elasticsearchContainer = new ESContainerSetup();

  private static final int REDIS_PORT = 6379;

  @Container
  public static GenericContainer<?> redisContainer =
      new GenericContainer<>(DockerImageName.parse( "redis:6.0"))
          .withExposedPorts(REDIS_PORT);

  public static class RedisEnvInit implements
      ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext ctx) {
      TestPropertyValues.of(
          "spring.redis.host=" + redisContainer.getHost(),
          "spring.redis.port=" + redisContainer.getMappedPort(REDIS_PORT)
      ).applyTo(ctx.getEnvironment());
    }
  }

  @TestConfiguration
  @Import(CassandraContainerSetup.class)
  public static class TestConfig {

    @Bean
    CassandraContainer<?> cassandraContainer() {
      return cassandraContainer;
    }
  }

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Autowired
  MetadataService metadataService;

  @Autowired
  ElasticSearchService elasticSearchService;

  @Autowired
  MeterRegistry meterRegistry;

  @MockBean
  IngestTrackingService ingestTrackingService;

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  RestHighLevelClient restHighLevelClient;

  @AfterEach
  void tearDown() throws IOException {
    cassandraTemplate.truncate(MetricName.class)
        .and(cassandraTemplate.truncate(SeriesSet.class))
        .block();

    DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("metrics");
    restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
  }

  @BeforeAll
  static void setUp() {
    elasticsearchContainer.start();
  }

  @AfterAll
  static void destroy() {
    elasticsearchContainer.stop();
  }

  @BeforeEach
  void testIsContainerRunning() throws IOException {
    assertTrue(elasticsearchContainer.isRunning());

    CreateIndexRequest request = new CreateIndexRequest("metrics");
    String mapping = "{\n    \"properties\": {\n        \"id\": {\n          \"type\": \"keyword\"\n        },\n        \"metricName\": {\n          \"type\": \"keyword\"\n        },\n        \"tenant\": {\n          \"type\": \"keyword\"\n        }\n    }\n  }";
    request.mapping(mapping, XContentType.JSON);
    log.info("cluster info {} ",restHighLevelClient.cluster().toString());
    restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
  }

  @Test
  void storeMetadata() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = randomAlphabetic(5);
    // NOTE: the series sets are normally encoded as a hash in the seriesSetHash field, but
    // using metric,tagK=tagV encoding for ease of debugging test failures
    final String seriesSet1 = unhashedSeriesSet(metricName, Map.of(
        "host", "h-1",
        "os", "linux",
        "deployment", "prod"
    ));
    final String seriesSet2 = unhashedSeriesSet(metricName, Map.of(
        "host", "h-2",
        "os", "windows",
        "deployment", "prod"
    ));
    final String seriesSet3 = unhashedSeriesSet(metricName, Map.of(
        "host", "h-3",
        "os", "linux",
        "deployment", "dev"
    ));

    store(tenantId, metricName, "linux", "h-1", "prod");
    store(tenantId, metricName, "linux", "h-1", "prod");
    store(tenantId, metricName, "linux", "h-1", "prod");

    store(tenantId, metricName, "windows", "h-2", "prod");
    store(tenantId, metricName, "windows", "h-2", "prod");
    store(tenantId, metricName, "windows", "h-2", "prod");

    store(tenantId, metricName, "linux", "h-3", "dev");
    store(tenantId, metricName, "linux", "h-3", "dev");
    store(tenantId, metricName, "linux", "h-3", "dev");


    assertThat(cassandraTemplate.select(Query.query(), SeriesSet.class).collectList().block())
        .containsExactlyInAnyOrder(
            seriesSet(tenantId, metricName, "deployment", "dev", seriesSet3),
            seriesSet(tenantId, metricName, "deployment", "prod", seriesSet1),
            seriesSet(tenantId, metricName, "deployment", "prod", seriesSet2),
            seriesSet(tenantId, metricName, "host", "h-1", seriesSet1),
            seriesSet(tenantId, metricName, "host", "h-2", seriesSet2),
            seriesSet(tenantId, metricName, "host", "h-3", seriesSet3),
            seriesSet(tenantId, metricName, "os", "linux", seriesSet3),
            seriesSet(tenantId, metricName, "os", "linux", seriesSet1),
            seriesSet(tenantId, metricName, "os", "windows", seriesSet2)
        );

    assertThat(cassandraTemplate.count(SeriesSet.class).block()).isEqualTo(9);

    assertThat(
        cassandraTemplate.select("SELECT metric_name FROM metric_names", String.class).collectList()
            .block()
    ).containsExactly(metricName);

    MetricDTO metricDTO1 = new MetricDTO(metricName, Map.of(
        "host", "h-1", "os", "linux", "deployment", "prod"
    ));
    MetricDTO metricDTO2 = new MetricDTO(metricName, Map.of(
        "host", "h-2", "os", "windows", "deployment", "prod"
    ));
    MetricDTO metricDTO3 = new MetricDTO(metricName, Map.of(
        "host", "h-3", "os", "linux", "deployment", "dev"
    ));
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2, metricDTO3);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    Thread.sleep(1000);
    Criteria criteria = new Criteria();
    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }
  private void store(String tenantId, String metricName,
                     String os, String host,
                     String deployment) {
    final Map<String, String> tags = Map.of(
        "os", os,
        "host", host,
        "deployment", deployment
    );
    final Metric metric = new Metric()
        .setTimestamp(Instant.now())
        .setValue(Math.random())
        .setMetric(metricName)
        .setTags(tags);

    Mono.ignoreElements(
        metadataService.storeMetadata(
            tenantId,
            unhashedSeriesSet(metricName, tags), metric
        )
    )
        .block();
  }

  @Test
  void getTenants() {
    final List<String> tenantIds = IntStream.range(0, 10)
        .mapToObj(value -> RandomStringUtils.randomAlphanumeric(10))
        .collect(Collectors.toList());

    Flux.fromIterable(tenantIds)
        .flatMap(tenantId ->
            cassandraTemplate.insert(new MetricName()
                .setTenant(tenantId)
                .setMetricName(RandomStringUtils.randomAlphanumeric(5))
            )
        )
        .blockLast();

    final List<String> result = metadataService.getTenants()
        .block();

    assertThat(result).containsExactlyInAnyOrderElementsOf(tenantIds);
  }

  @Nested
  public class resolveSeriesSetHash {

    @Test
    void exists() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
      final String seriesSetHash = randomAlphabetic(22);
      final String metricName = randomAlphabetic(5);
      final Map<String,String> tags = Map.of(
          randomAlphabetic(5),
          randomAlphabetic(5)
      );

      cassandraTemplate.insert(
          new SeriesSetHash()
              .setTenant(tenantId)
              .setSeriesSetHash(seriesSetHash)
              .setMetricName(metricName)
              .setTags(tags)
      )
          .block();

      final MetricNameAndTags result = metadataService.resolveSeriesSetHash(tenantId, seriesSetHash)
          .block();

      assertThat(result).isNotNull();
      assertThat(result.getMetricName()).isEqualTo(metricName);
      assertThat(result.getTags()).isEqualTo(tags);
    }

    @Test
    void missing() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
      final String seriesSetHash = randomAlphabetic(22);

      assertThatThrownBy(() ->
          metadataService.resolveSeriesSetHash(tenantId, seriesSetHash)
          .block()
      )
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(seriesSetHash);
    }
  }

  @Test
  void getMetricNames() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final List<String> metricNames = List.of(
        randomAlphabetic(5),
        randomAlphabetic(5)
    );

    Flux.fromIterable(metricNames)
        .flatMap(metricName -> cassandraTemplate.insert(
            new MetricName()
                .setTenant(tenantId)
                .setMetricName(metricName)
        ))
        .blockLast();

    // and insert another for some other tenant
    cassandraTemplate.insert(
        new MetricName()
            .setTenant("not-" + tenantId)
            .setMetricName(randomAlphabetic(5))
    )
        .block();

    final List<String> results = metadataService.getMetricNames(tenantId)
        .block();

    assertThat(results).containsExactlyInAnyOrderElementsOf(metricNames);
  }

  @Test
  void getMetricNamesFailed() {
    try {
      metadataService.getMetricNames("").block();
    } catch (Exception ignored) {
      final double value = meterRegistry.get("ceres.db.operation.errors")
          .tags("type", "read")
          .counter().count();
      assertThat(value).isEqualTo(1);
    }
  }

  private SeriesSet seriesSet(String tenantId, String metricName, String tagKey, String tagValue,
                              String seriesSetHash) {
    return new SeriesSet().setTenant(tenantId).setMetricName(metricName).setTagKey(tagKey)
        .setTagValue(tagValue).setSeriesSetHash(seriesSetHash);
  }

  @Test
  void getMetricsAndTagsAndMetadata() {
    Duration granularityPT2M = Duration.ofMinutes(2);
    Duration granularityPT1M = Duration.ofMinutes(1);

    final TsdbFilter filter = new TsdbFilter()
            .setType(FilterType.literal_or)
            .setTagk("host")
            .setFilter("h-1|h-2");

    TsdbQueryRequest tsdbQueryRequest1 = new TsdbQueryRequest()
            .setMetric("cpu_idle")
            .setDownsample("2m-avg")
            .setFilters(List.of(filter));

    TsdbQueryRequest tsdbQueryRequest2 = new TsdbQueryRequest()
            .setMetric("cpu_active")
            .setDownsample("1m-sum")
            .setFilters(List.of(filter));

    List<Granularity> granularities = List.of(granularity(1, 12), granularity(2, 24));

    String tenant = "t-1";
    Instant start = Instant.now();
    Instant end = Instant.now();
    List<TsdbQuery> results = metadataService.getTsdbQueries(
            List.of(tsdbQueryRequest1, tsdbQueryRequest2), granularities).collectList().block();

    assertEquals(4, results.size());

    int metricCpuIdle = 0;
    int metricCpuActive = 0;
    boolean cpuActiveH1 = false;
    boolean cpuActiveH2 = false;
    boolean cpuIdleH1 = false;
    boolean cpuIdleH2 = false;

    for (TsdbQuery result : results) {
      if (result.getMetricName().equals("cpu_idle")) {
        metricCpuIdle++;
        assertThat(result.getGranularity()).isEqualTo(granularityPT2M);
        assertThat(result.getAggregator()).isEqualTo(Aggregator.avg);
        if (result.getTags().get("host").equals("h-1")) {
          cpuIdleH1 = true;
        } else if (result.getTags().get("host").equals("h-2")) {
          cpuIdleH2 = true;
        }
      } else if (result.getMetricName().equals("cpu_active")) {
        metricCpuActive++;
        assertThat(result.getGranularity()).isEqualTo(granularityPT1M);
        assertThat(result.getAggregator()).isEqualTo(Aggregator.sum);
        if (result.getTags().get("host").equals("h-1")) {
          cpuActiveH1 = true;
        } else if (result.getTags().get("host").equals("h-2")) {
          cpuActiveH2 = true;
        }
      }
    }

    assertEquals(2, metricCpuIdle);
    assertEquals(2, metricCpuActive);
    assertTrue(cpuActiveH1 && cpuActiveH2 && cpuIdleH1 && cpuIdleH2);
  }

  @Test
  void getMetricsAndTagsAndMetadataRawQuery() {
    final TsdbFilter filter = new TsdbFilter()
            .setType(FilterType.literal_or)
            .setTagk("host")
            .setFilter("h-1");

    TsdbQueryRequest tsdbQueryRequest = new TsdbQueryRequest()
            .setMetric("cpu_idle")
            .setDownsample(null)
            .setFilters(List.of(filter));

    String tenant = "t-1";
    Instant start = Instant.now();
    Instant end = Instant.now();

    List<TsdbQuery> results = metadataService.getTsdbQueries(
            List.of(tsdbQueryRequest), Collections.emptyList()).collectList().block();
    TsdbQuery result = results.get(0);

    assertEquals(1, results.size());
    assertTrue(result.getMetricName().equals("cpu_idle"));
    assertTrue(result.getTags().get("host").equals("h-1"));
    assertEquals(null, result.getGranularity());
    assertEquals(Aggregator.raw, result.getAggregator());
  }

  @Test
  void getMetricNameAndTags() {
    MetricNameAndMultiTags metricNameAndTags = metadataService.getMetricNameAndTags("cpu_active");
    assertEquals("cpu_active", metricNameAndTags.getMetricName());
    assertEquals(List.of(), metricNameAndTags.getTags());

    metricNameAndTags = metadataService.getMetricNameAndTags("cpu_active{host=*}");
    assertEquals("cpu_active", metricNameAndTags.getMetricName());
    assertEquals(List.of(Map.of("host", "*")), metricNameAndTags.getTags());

    metricNameAndTags = metadataService.getMetricNameAndTags("cpu_active{os=linux,host=*,*=windows}");
    assertEquals("cpu_active", metricNameAndTags.getMetricName());
    assertEquals(List.of(
            Map.of("os", "linux"),
            Map.of("host", "*"),
            Map.of("*", "windows")), metricNameAndTags.getTags());
  }

  private String unhashedSeriesSet(String metricName, Map<String, String> tags) {
    return metricName + "," +
        tags.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> entry.getKey()+"="+entry.getValue())
            .collect(Collectors.joining(","));
  }

  private Granularity granularity(int minutes, int ttlHours) {
    return new Granularity()
        .setWidth(Duration.ofMinutes(minutes))
        .setTtl(Duration.ofHours(ttlHours));
  }
}
