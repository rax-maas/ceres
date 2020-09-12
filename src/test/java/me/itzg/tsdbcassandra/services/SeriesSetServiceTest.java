package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.model.Metric;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@SpringBootTest
@Testcontainers
class SeriesSetServiceTest {

  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>();

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Autowired
  SeriesSetService seriesSetService;

  @Test
  void buildSeriesSet() {
    final String result = seriesSetService.buildSeriesSet("name_here", Map.of(
        "os", "linux",
        "host", "server-1",
        "deployment", "prod"
    ));

    assertThat(result).isEqualTo("name_here,deployment=prod,host=server-1,os=linux");
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
        .setTs(Instant.now())
        .setValue(Math.random())
        .setTenant(tenantId)
        .setMetricName(metricName)
        .setTags(Map.of(
            "os", os,
            "host", host,
            "deployment", deployment
        ));

    Mono.ignoreElements(
        seriesSetService.storeMetadata(
            metric, seriesSetService.buildSeriesSet(metric.getMetricName(), metric.getTags())
        )
    )
        .block();
  }

  private SeriesSet seriesSet(String tenantId, String metricName, String tagKey, String tagValue,
                              String tagPart) {
    return new SeriesSet().setTenant(tenantId).setMetricName(metricName).setTagKey(tagKey)
        .setTagValue(tagValue).setSeriesSet(metricName + "," + tagPart);
  }

  @TestConfiguration
  @Import(CassandraContainerSetup.class)
  public static class TestConfig {

    @Bean
    CassandraContainer<?> cassandraContainer() {
      return cassandraContainer;
    }
  }
}
