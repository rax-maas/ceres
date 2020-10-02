package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import me.itzg.tsdbcassandra.entities.MetricName;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.model.Metric;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
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
import reactor.core.publisher.Flux;
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
    cassandraTemplate.truncate(MetricName.class)
        .and(cassandraTemplate.truncate(SeriesSet.class))
        .block();
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

  private SeriesSet seriesSet(String tenantId, String metricName, String tagKey, String tagValue,
                              String tagPart) {
    return new SeriesSet().setTenant(tenantId).setMetricName(metricName).setTagKey(tagKey)
        .setTagValue(tagValue).setSeriesSet(metricName + "," + tagPart);
  }

}
