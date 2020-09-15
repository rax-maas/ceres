package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import me.itzg.tsdbcassandra.entities.Aggregator;
import me.itzg.tsdbcassandra.entities.MetricName;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.entities.TagKey;
import me.itzg.tsdbcassandra.entities.TagValue;
import me.itzg.tsdbcassandra.model.Metric;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class MetadataServiceTest {
  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>();
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
  SeriesSetService seriesSetService;

  @AfterEach
  void tearDown() {
    cassandraTemplate.truncate(MetricName.class);
    cassandraTemplate.truncate(TagKey.class);
    cassandraTemplate.truncate(TagValue.class);
    cassandraTemplate.truncate(SeriesSet.class);
  }

  @Test
  void storeMetadata() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
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
            seriesSet(
                tenantId, metricName, "deployment", "dev", "deployment=dev,host=h-3,os=linux"),
            seriesSet(
                tenantId, metricName, "deployment", "prod", "deployment=prod,host=h-1,os=linux"),
            seriesSet(
                tenantId, metricName, "deployment", "prod", "deployment=prod,host=h-2,os=windows"),
            seriesSet(tenantId, metricName, "host", "h-1", "deployment=prod,host=h-1,os=linux"),
            seriesSet(tenantId, metricName, "host", "h-2", "deployment=prod,host=h-2,os=windows"),
            seriesSet(tenantId, metricName, "host", "h-3", "deployment=dev,host=h-3,os=linux"),
            seriesSet(tenantId, metricName, "os", "linux", "deployment=dev,host=h-3,os=linux"),
            seriesSet(tenantId, metricName, "os", "linux", "deployment=prod,host=h-1,os=linux"),
            seriesSet(tenantId, metricName, "os", "windows", "deployment=prod,host=h-2,os=windows")
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
    final Metric metric = new Metric()
        .setTimestamp(Instant.now())
        .setValue(Math.random())
        .setMetric(metricName)
        .setTags(Map.of(
            "os", os,
            "host", host,
            "deployment", deployment
        ));

    Mono.ignoreElements(
        metadataService.storeMetadata(
            tenantId,
            metric,
            seriesSetService.buildSeriesSet(metric.getMetric(), metric.getTags())
        )
    )
        .block();
  }

  @Nested
  class metricNameHasAggregator {

    @Test
    void exists() {
      final String tenant = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphanumeric(10);

      insertOne(tenant, metricName);

      final Boolean result = metadataService
          .metricNameHasAggregator(tenant, metricName, Aggregator.min)
          .block();

      assertThat(result).isTrue();
    }

    private void insertOne(String tenant, String metricName) {
      cassandraTemplate.insert(
          new MetricName()
              .setTenant(tenant)
              .setMetricName(metricName)
              .setAggregators(Set.of(Aggregator.raw, Aggregator.min, Aggregator.max))
      ).block();
    }

    @Test
    void existsButNotAggregator() {
      final String tenant = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphanumeric(10);

      insertOne(tenant, metricName);

      final Boolean result = metadataService
          .metricNameHasAggregator(tenant, metricName, Aggregator.avg)
          .block();

      assertThat(result).isFalse();
    }

    @Test
    void notExists() {
      final String tenant = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphanumeric(10);

      insertOne(tenant, metricName);

      final Boolean result = metadataService
          .metricNameHasAggregator(tenant,
              // query for metric name that doesn't exist
              metricName+"_not_exists",
              Aggregator.min)
          .block();

      assertThat(result).isFalse();
    }
  }

  private SeriesSet seriesSet(String tenantId, String metricName, String tagKey, String tagValue,
                              String tagPart) {
    return new SeriesSet().setTenant(tenantId).setMetricName(metricName).setTagKey(tagKey)
        .setTagValue(tagValue).setSeriesSet(metricName + "," + tagPart);
  }

}
