package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.Map;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.model.Metric;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.http.HttpStatus.Series;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class IngestServiceTest {

  protected static final String TENANT = "t-1";
  protected static final String METRIC_NAME = "cpu_idle";
  @Container
  public static CassandraContainer cassandraContainer = new CassandraContainer();

  @TestConfiguration
  public static class TestConfig {
    @Bean
    public CqlSessionBuilderCustomizer cqlSessionBuilderCustomizer() {
      return cqlSessionBuilder -> {

        final Cluster cluster = cassandraContainer.getCluster();
        try (Session session = cluster.connect()) {
          session.execute("CREATE KEYSPACE IF NOT EXISTS tsdb "
              + "    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        }

        cqlSessionBuilder.addContactPoint(
            new InetSocketAddress(cassandraContainer.getHost(), cassandraContainer.getMappedPort(CassandraContainer.CQL_PORT))
        );
      };
    }
  }

  @Autowired
  IngestService ingestService;

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Test
  void ingestingWorks() {
    ingest(20, 20, "linux", "h-1", "prod");
    ingest(10, 30, "linux", "h-1", "prod");
    ingest(0, 40, "linux", "h-1", "prod");

    ingest(20, 25, "windows", "h-2", "prod");
    ingest(10, 35, "windows", "h-2", "prod");
    ingest(0, 45, "windows", "h-2", "prod");

    ingest(20, 50, "linux", "h-3", "dev");
    ingest(10, 60, "linux", "h-3", "dev");
    ingest(0, 70, "linux", "h-3", "dev");

    assertThat(cassandraTemplate.count(SeriesSet.class).block()).isEqualTo(9);
    assertThat(
        cassandraTemplate.select("SELECT metric_name FROM metric_names", String.class).collectList()
            .block()
    ).containsExactly(METRIC_NAME);
  }

  private void ingest(int timeOffset, int value, String os, String host, String deployment) {
    ingestService.ingest(
        new Metric()
            .setTs(Instant.now().minusSeconds(timeOffset))
            .setValue(value)
            .setTenant(TENANT)
            .setMetricName(METRIC_NAME)
            .setTags(Map.of(
                "os", os,
                "host", host,
                "deployment", deployment
            ))
    )
        .block();
  }
}
