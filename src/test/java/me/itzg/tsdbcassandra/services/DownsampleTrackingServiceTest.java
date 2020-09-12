package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import me.itzg.tsdbcassandra.entities.PendingDownsampleSet;
import me.itzg.tsdbcassandra.model.Metric;
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

@SpringBootTest(properties = {
    // make sure the default isn't contacted
    "spring.data.cassandra.contact-points=",
    "app.downsample.enabled=true",
    "app.downsample.partitions=64",
    "app.downsample.time-slot-width=2"
})
@Testcontainers
class DownsampleTrackingServiceTest {

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
  DownsampleTrackingService downsampleTrackingService;
  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @Test
  void track() throws InterruptedException {
    // need stable value to keep partition result stable for assertion
    final String tenantId = "tenant-1";
    final String metricName = "some_metric";
    final String seriesSet = metricName + ",deployment=prod,host=h-1,os=linux";

    Mono.from(
        downsampleTrackingService.track(
            new Metric()
                .setTs(Instant.parse("2020-09-12T19:42:23.658447900Z"))
                .setValue(Math.random())
                .setTenant(tenantId)
                .setMetricName(metricName)
                .setTags(Map.of(
                    "os", "linux",
                    "host", "h-1",
                    "deployment", "prod"
                )),
            seriesSet
        )
    ).block();

    final List<PendingDownsampleSet> results = cassandraTemplate.select(
        Query.query(),
        PendingDownsampleSet.class
    ).collectList().block();

    assertThat(results).isNotNull();
    assertThat(results)
        .usingElementComparatorIgnoringFields("lastTouch")
        .containsExactly(
            new PendingDownsampleSet()
                .setPartition(50)
                .setTenant(tenantId)
                .setSeriesSet(seriesSet)
                .setTimeSlot(Instant.parse("2020-09-12T18:00:00.000Z"))
        );
    assertThat(results.get(0).getLastTouch()).isAfter(Instant.EPOCH);

    Thread.sleep(1000);

    // store another a second later
    Mono.from(
        downsampleTrackingService.track(
            new Metric()
                .setTs(Instant.parse("2020-09-12T19:59:59.1Z"))
                .setValue(Math.random())
                .setTenant(tenantId)
                .setMetricName(metricName)
                .setTags(Map.of(
                    "os", "linux",
                    "host", "h-1",
                    "deployment", "prod"
                )),
            seriesSet
        )
    ).block();

    final List<PendingDownsampleSet> nextResults = cassandraTemplate.select(
        Query.query(),
        PendingDownsampleSet.class
    ).collectList().block();

    assertThat(nextResults).isNotNull();
    assertThat(nextResults)
        .usingElementComparatorIgnoringFields("lastTouch")
        .containsExactly(
            new PendingDownsampleSet()
                .setPartition(50)
                .setTenant(tenantId)
                .setSeriesSet(seriesSet)
                .setTimeSlot(Instant.parse("2020-09-12T18:00:00.000Z"))
        );
    assertThat(nextResults.get(0).getLastTouch()).isAfter(results.get(0).getLastTouch());
  }
}
