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

import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricNameAndTags;
import com.rackspace.ceres.app.services.MetadataServiceTest.RedisEnvInit;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
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
@ActiveProfiles("test")
@ContextConfiguration(initializers = RedisEnvInit.class)
@Testcontainers
class MetadataServiceTest {
  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>(
      CassandraContainerSetup.DOCKER_IMAGE);

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

  @AfterEach
  void tearDown() {
    cassandraTemplate.truncate(MetricName.class)
        .and(cassandraTemplate.truncate(SeriesSet.class))
        .block();
  }

  @Test
  void storeMetadata() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = randomAlphabetic(5);
    // NOTE: the series sets are normally encoded as a hash in the seriestSetHash field, but
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
            unhashedSeriesSet(metricName, tags), metric.getMetric(), metric.getTags()
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
  void getTagKeys() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = randomAlphabetic(5);
    final String tagK1 = randomAlphabetic(5);
    final String tagK2 = randomAlphabetic(5);
    final String tagV1 = randomAlphabetic(5);
    final String tagV2 = randomAlphabetic(5);

    cassandraTemplate.insert(
        seriesSet(tenantId, metricName, tagK1, tagV1, randomAlphabetic(5))
    ).block();
    cassandraTemplate.insert(
        seriesSet(tenantId, metricName, tagK2, tagV2, randomAlphabetic(5))
    ).block();
    // and one with different metric name
    cassandraTemplate.insert(
        seriesSet(tenantId, "not-"+metricName, randomAlphabetic(5), randomAlphabetic(5), randomAlphabetic(5))
    ).block();

    final List<String> results = metadataService.getTagKeys(tenantId, metricName)
        .block();

    assertThat(results).containsExactlyInAnyOrder(tagK1, tagK2);
  }

  @Test
  void getTagValues() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = randomAlphabetic(5);
    final String tagK = randomAlphabetic(5);
    final String tagV1 = randomAlphabetic(5);
    final String tagV2 = randomAlphabetic(5);

    cassandraTemplate.insert(
        seriesSet(tenantId, metricName, tagK, tagV1, randomAlphabetic(5))
    ).block();
    cassandraTemplate.insert(
        seriesSet(tenantId, metricName, tagK, tagV2, randomAlphabetic(5))
    ).block();
    // and one with different tag key
    cassandraTemplate.insert(
        seriesSet(tenantId, metricName, "not-"+tagK, randomAlphabetic(5), randomAlphabetic(5))
    ).block();

    final List<String> results = metadataService.getTagValues(tenantId, metricName, tagK)
        .block();

    assertThat(results).containsExactlyInAnyOrder(tagV1, tagV2);
  }

  private SeriesSet seriesSet(String tenantId, String metricName, String tagKey, String tagValue,
                              String seriesSetHash) {
    return new SeriesSet().setTenant(tenantId).setMetricName(metricName).setTagKey(tagKey)
        .setTagValue(tagValue).setSeriesSetHash(seriesSetHash);
  }

  private String unhashedSeriesSet(String metricName, Map<String, String> tags) {
    return metricName + "," +
        tags.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .map(entry -> entry.getKey()+"="+entry.getValue())
            .collect(Collectors.joining(","));
  }

}
