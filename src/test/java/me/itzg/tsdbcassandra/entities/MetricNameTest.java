package me.itzg.tsdbcassandra.entities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.cassandra.core.query.Criteria.where;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.net.InetSocketAddress;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class MetricNameTest {
  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>();

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
  ReactiveCassandraTemplate cassandraTemplate;

  @Test
  void upsertWithNulledFields() {
    final MetricName resultWithRaw = cassandraTemplate.update(
        Query.query(
            where("tenant").is("t-1"),
            where("metricName").is("cpu_usage")
        ),
        Update.empty().addTo("aggregators").append(Aggregator.raw),
        MetricName.class
        ).thenMany(cassandraTemplate.select(
        Query.query(
            where("tenant").is("t-1"),
            where("metricName").is("cpu_usage")
        ),
        MetricName.class
    ))
        .blockFirst();

    assertThat(resultWithRaw).isNotNull();
    assertThat(resultWithRaw.getAggregators()).containsOnly(Aggregator.raw);

    final MetricName resultWithDownsampled = cassandraTemplate.update(
        Query.query(
            where("tenant").is("t-1"),
            where("metricName").is("cpu_usage")
        ),
        Update.empty().addTo("aggregators").appendAll(Aggregator.min, Aggregator.max),
        MetricName.class
    ).thenMany(cassandraTemplate.select(
        Query.query(
            where("tenant").is("t-1"),
            where("metricName").is("cpu_usage")
        ),
        MetricName.class
    ))
        .blockFirst();


    assertThat(resultWithDownsampled).isNotNull();
    assertThat(resultWithDownsampled.getAggregators()).containsExactlyInAnyOrder(
        Aggregator.raw, Aggregator.min, Aggregator.max
    );
  }
}
