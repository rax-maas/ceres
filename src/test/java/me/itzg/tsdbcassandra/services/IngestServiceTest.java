package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.data.cassandra.core.query.Criteria.where;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import me.itzg.tsdbcassandra.entities.DataRaw;
import me.itzg.tsdbcassandra.model.Metric;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
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

  @MockBean
  SeriesSetService seriesSetService;

  @Autowired
  IngestService ingestService;

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Test
  void ingestingWorks() {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String seriesSet = metricName + ",deployment=prod,host=h-1,os=linux";

    when(seriesSetService.storeMetadata(any(), any()))
        .thenReturn(Mono.empty());
    when(seriesSetService.buildSeriesSet(any(), any()))
        .thenReturn(seriesSet);

    final Metric metric = ingestService.ingest(
        new Metric()
            .setTs(Instant.parse("2020-09-12T18:42:23.658447900Z"))
            .setValue(Math.random())
            .setTenant(tenantId)
            .setMetricName(metricName)
            .setTags(Map.of(
                "os", "linux",
                "host", "h-1",
                "deployment", "prod"
            ))
    )
        .block();
    
    assertThat(metric).isNotNull();

    final List<DataRaw> results = cassandraTemplate.select(
        Query.query(
            where("tenant").is(tenantId),
            where("seriesSet").is(seriesSet)
        ),
        DataRaw.class
    ).collectList().block();

    assertThat(results).isNotNull();
    assertThat(results).hasSize(1);
    assertThat(results.get(0).getTenant()).isEqualTo(tenantId);
    // only millisecond resolution retained by cassandra
    assertThat(results.get(0).getTs()).isEqualTo("2020-09-12T18:42:23.658Z");
    assertThat(results.get(0).getValue()).isEqualTo(metric.getValue());

    verify(seriesSetService).buildSeriesSet(metricName, metric.getTags());
    verify(seriesSetService).storeMetadata(metric, seriesSet);

    verifyNoMoreInteractions(seriesSetService);
  }

}
