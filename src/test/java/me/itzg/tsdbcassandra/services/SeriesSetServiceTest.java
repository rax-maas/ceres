package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class SeriesSetServiceTest {
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
}
