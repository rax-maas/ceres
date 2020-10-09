package me.itzg.ceres;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import java.net.InetSocketAddress;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.testcontainers.containers.CassandraContainer;

/**
 * To be imported from a unit test that <code>@Testcontainers</code> activated. For example:
 * <pre>
 &#64;Container
 public static CassandraContainer&lt;?&gt; cassandraContainer = new CassandraContainer&lt;&gt;();
 &#64;TestConfiguration
 &#64;Import(CassandraContainerSetup.class)
 public static class TestConfig {
   &#64;Bean
   CassandraContainer&lt;?&gt; cassandraContainer() {
     return cassandraContainer;
   }
 }
  * </pre>
 */
@TestConfiguration
public class CassandraContainerSetup {

  @Bean
  public CqlSessionBuilderCustomizer cqlSessionBuilderCustomizer(CassandraContainer<?> cassandraContainer) {
    return CassandraContainerSetup.setupSession(cassandraContainer);
  }

  public static CqlSessionBuilderCustomizer setupSession(CassandraContainer<?> cassandraContainer) {
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
