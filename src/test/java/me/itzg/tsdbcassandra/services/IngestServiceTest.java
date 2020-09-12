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

@SpringBootTest
@Testcontainers
class IngestServiceTest {

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
  IngestService ingestService;

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Test
  void ingestingWorks() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    ingest(tenantId,metricName, 20, 20, "linux", "h-1", "prod");
    ingest(tenantId, metricName, 10, 30, "linux", "h-1", "prod");
    ingest(tenantId, metricName, 0, 40, "linux", "h-1", "prod");

    ingest(tenantId, metricName, 20, 25, "windows", "h-2", "prod");
    ingest(tenantId, metricName, 10, 35, "windows", "h-2", "prod");
    ingest(tenantId, metricName, 0, 45, "windows", "h-2", "prod");

    ingest(tenantId, metricName, 20, 50, "linux", "h-3", "dev");
    ingest(tenantId, metricName, 10, 60, "linux", "h-3", "dev");
    ingest(tenantId, metricName, 0, 70, "linux", "h-3", "dev");

    assertThat(cassandraTemplate.select(Query.query(), SeriesSet.class).collectList().block())
        .containsExactlyInAnyOrder(
            seriesSet(tenantId, metricName, "deployment", "dev", "deployment=dev,host=h-3,os=linux"),
            seriesSet(tenantId, metricName, "deployment", "prod", "deployment=prod,host=h-1,os=linux"),
            seriesSet(tenantId, metricName, "deployment", "prod", "deployment=prod,host=h-2,os=windows"),
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

  private SeriesSet seriesSet(String tenantId, String metricName, String tagKey, String tagValue,
                              String tagPart) {
    return new SeriesSet().setTenant(tenantId).setMetricName(metricName).setTagKey(tagKey)
        .setTagValue(tagValue).setSeriesSet(metricName+ "," + tagPart);
  }

  private void ingest(String tenantId, String metricName, int timeOffset, int value,
                      String os, String host,
                      String deployment) {
    ingestService.ingest(
        new Metric()
            .setTs(Instant.now().minusSeconds(timeOffset))
            .setValue(value)
            .setTenant(tenantId)
            .setMetricName(metricName)
            .setTags(Map.of(
                "os", os,
                "host", host,
                "deployment", deployment
            ))
    )
        .block();
  }
}
