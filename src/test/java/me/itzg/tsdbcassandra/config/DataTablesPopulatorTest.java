package me.itzg.tsdbcassandra.config;

import me.itzg.tsdbcassandra.CassandraContainerSetup;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest(properties = {
    "app.downsample.granularities[0].width=1m",
    "app.downsample.granularities[0].ttl=2d",
    "app.downsample.granularities[1].width=5m",
    "app.downsample.granularities[1].ttl=14d"
})
@ActiveProfiles("test")
@Testcontainers
class DataTablesPopulatorTest {
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
  ReactiveSessionFactory sessionFactory;

  @Test
  void populate() {
  }
}