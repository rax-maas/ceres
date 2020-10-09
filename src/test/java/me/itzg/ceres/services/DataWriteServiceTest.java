package me.itzg.ceres.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.Row;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import me.itzg.ceres.CassandraContainerSetup;
import me.itzg.ceres.model.Metric;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
class DataWriteServiceTest {

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
  MetadataService metadataService;

  @MockBean
  DownsampleTrackingService downsampleTrackingService;

  @Autowired
  SeriesSetService seriesSetService;

  @Autowired
  DataWriteService dataWriteService;

  @Autowired
  ReactiveCqlTemplate cqlTemplate;

  @Nested
  class ingest {
    @Test
    void single() {
      final String tenantId = RandomStringUtils.randomAlphanumeric(10);
      final String metricName = RandomStringUtils.randomAlphabetic(5);
      final String seriesSet = metricName + ",deployment=prod,host=h-1,os=linux";

      when(metadataService.storeMetadata(any(), any(), any()))
          .thenReturn(Mono.empty());

      when(downsampleTrackingService.track(any(), anyString(), any()))
          .thenReturn(Mono.empty());

      final Metric metric = dataWriteService.ingest(
          tenantId,
          new Metric()
              .setTimestamp(Instant.parse("2020-09-12T18:42:23.658447900Z"))
              .setValue(Math.random())
              .setMetric(metricName)
              .setTags(Map.of(
                  "os", "linux",
                  "host", "h-1",
                  "deployment", "prod"
              ))
      )
          .block();

      assertThat(metric).isNotNull();

      assertViaQuery(tenantId, seriesSet, metric);

      verify(metadataService).storeMetadata(tenantId, metric, seriesSet);

      verify(downsampleTrackingService).track(tenantId, seriesSet, metric.getTimestamp());

      verifyNoMoreInteractions(metadataService, downsampleTrackingService);
    }

    @Test
    void multi() {
      final String tenant1 = RandomStringUtils.randomAlphanumeric(10);
      final String tenant2 = RandomStringUtils.randomAlphanumeric(10);
      final String metricName1 = RandomStringUtils.randomAlphabetic(5);
      final String metricName2 = RandomStringUtils.randomAlphabetic(5);
      final String seriesSet1 = metricName1 + ",deployment=prod,host=h-1,os=linux";
      final String seriesSet2 = metricName2 + ",deployment=prod,host=h-1,os=linux";

      when(metadataService.storeMetadata(any(), any(), any()))
          .thenReturn(Mono.empty());

      when(downsampleTrackingService.track(any(), anyString(), any()))
          .thenReturn(Mono.empty());

      final Metric metric1 = new Metric()
          .setTimestamp(Instant.parse("2020-09-12T18:42:23.658447900Z"))
          .setValue(Math.random())
          .setMetric(metricName1)
          .setTags(Map.of(
              "os", "linux",
              "host", "h-1",
              "deployment", "prod"
          ));
      final Metric metric2 = new Metric()
          .setTimestamp(Instant.parse("2020-09-12T18:42:23.658447900Z"))
          .setValue(Math.random())
          .setMetric(metricName2)
          .setTags(Map.of(
              "os", "linux",
              "host", "h-1",
              "deployment", "prod"
          ));

      dataWriteService.ingest(Flux.just(
          Tuples.of(tenant1, metric1),
          Tuples.of(tenant2, metric2)
      )).then().block();

      assertViaQuery(tenant1, seriesSet1, metric1);
      assertViaQuery(tenant2, seriesSet2, metric2);

      verify(metadataService).storeMetadata(tenant1, metric1, seriesSet1);
      verify(metadataService).storeMetadata(tenant2, metric2, seriesSet2);

      verify(downsampleTrackingService).track(tenant1, seriesSet1, metric1.getTimestamp());
      verify(downsampleTrackingService).track(tenant2, seriesSet2, metric2.getTimestamp());

      verifyNoMoreInteractions(metadataService, downsampleTrackingService);
    }

    private void assertViaQuery(String tenant, String seriesSet, Metric metric) {
      final List<Row> results = cqlTemplate.queryForRows(
          "SELECT ts, value FROM data_raw"
              + " WHERE tenant = ? AND series_set = ?",
          tenant, seriesSet
      ).collectList().block();

      assertThat(results).isNotNull();
      assertThat(results).hasSize(1);
      // only millisecond resolution retained by cassandra
      assertThat(results.get(0).getInstant(0)).isEqualTo("2020-09-12T18:42:23.658Z");
      assertThat(results.get(0).getDouble(1)).isEqualTo(metric.getValue());
    }

  }

}
