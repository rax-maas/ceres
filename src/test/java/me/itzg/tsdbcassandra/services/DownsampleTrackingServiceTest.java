package me.itzg.tsdbcassandra.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.springframework.data.cassandra.core.query.Criteria.where;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import me.itzg.tsdbcassandra.CassandraContainerSetup;
import me.itzg.tsdbcassandra.entities.PendingDownsampleSet;
import me.itzg.tsdbcassandra.model.Metric;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@SpringBootTest(properties = {
    "app.downsample.enabled=true",
    "app.downsample.partitions=64",
    "app.downsample.time-slot-width=PT2H",
    "app.downsample.last-touch-delay=PT2M"
})
@ActiveProfiles("test")
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

  @MockBean
  TimestampProvider timestampProvider;

  @Autowired
  DownsampleTrackingService downsampleTrackingService;
  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @AfterEach
  void tearDown() {
    // prune between tests
    cassandraTemplate.truncate(PendingDownsampleSet.class)
      .block();
  }

  @Test
  void track() {
    // need stable value to keep partition result stable for assertion
    final String tenantId = "tenant-1";
    final String metricName = "some_metric";
    final String seriesSet = metricName + ",deployment=prod,host=h-1,os=linux";

    final Instant touch1 = Instant.parse("2020-09-12T19:43:00.0Z");
    final Instant touch2 = Instant.parse("2020-09-12T19:44:00.0Z");
    when(timestampProvider.now())
        .thenReturn(touch1)
        // and for second call
        .thenReturn(touch2);

    final Metric metric = new Metric()
        .setTimestamp(Instant.parse("2020-09-12T19:42:23.658447900Z"))
        .setValue(Math.random())
        .setMetric(metricName)
        .setTags(Map.of(
            "os", "linux",
            "host", "h-1",
            "deployment", "prod"
        ));
    Mono.from(
        downsampleTrackingService.track(
            tenantId,
            seriesSet, metric.getTimestamp()
        )
    ).block();

    final List<PendingDownsampleSet> results = cassandraTemplate.select(
        Query.query(),
        PendingDownsampleSet.class
    ).collectList().block();

    assertThat(results).isNotNull();
    assertThat(results)
        .containsExactly(
            new PendingDownsampleSet()
                .setPartition(50)
                .setTenant(tenantId)
                .setSeriesSet(seriesSet)
                .setTimeSlot(Instant.parse("2020-09-12T18:00:00.000Z"))
                .setLastTouch(touch1)
        );

    // store another a second later
    final Metric metric1 = new Metric()
        .setTimestamp(Instant.parse("2020-09-12T19:59:59.1Z"))
        .setValue(Math.random())
        .setMetric(metricName)
        .setTags(Map.of(
            "os", "linux",
            "host", "h-1",
            "deployment", "prod"
        ));
    Mono.from(
        downsampleTrackingService.track(
            tenantId,
            seriesSet, metric1.getTimestamp()
        )
    ).block();

    final List<PendingDownsampleSet> nextResults = cassandraTemplate.select(
        Query.query(),
        PendingDownsampleSet.class
    ).collectList().block();

    assertThat(nextResults).isNotNull();
    assertThat(nextResults)
        .containsExactly(
            new PendingDownsampleSet()
                .setPartition(50)
                .setTenant(tenantId)
                .setSeriesSet(seriesSet)
                .setTimeSlot(Instant.parse("2020-09-12T18:00:00.000Z"))
                .setLastTouch(touch2)
        );
  }

  @Nested
  class retrieveReadyOnes {

    @Test
    void onlyIncludesRequestedPartition() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.000Z");

      final PendingDownsampleSet pending1 = createPending(timeSlot, 50, 2);
      final PendingDownsampleSet pending2 = createPending(timeSlot, 51, 1);

      cassandraTemplate.insert(pending1)
          .and(
              cassandraTemplate.insert(pending2)
          ).block();

      // current time is past time slot width and last touch delay
      when(timestampProvider.now())
          .thenReturn(timeSlot.plus(2, ChronoUnit.HOURS).plus(5, ChronoUnit.MINUTES));

      final List<PendingDownsampleSet> results = downsampleTrackingService
          .retrieveReadyOnes(50).collectList().block();

      assertThat(results).isNotNull();
      assertThat(results).containsExactly(pending1);
    }

    @Test
    void onlyOnesOutsideTimeslot() {
      final Instant timeSlot1 = Instant.parse("2020-09-12T18:00:00.000Z");
      final Instant timeSlot2 = timeSlot1.plus(2, ChronoUnit.HOURS);

      final PendingDownsampleSet pending1 = createPending(timeSlot1, 50, 2);
      final PendingDownsampleSet pending2 = createPending(timeSlot2, 50, 1);

      cassandraTemplate.insert(pending1)
          .and(
              cassandraTemplate.insert(pending2)
          ).block();

      // current time is just a little past start of timeSlot2
      when(timestampProvider.now())
          .thenReturn(timeSlot2.plus(5, ChronoUnit.MINUTES));

      final List<PendingDownsampleSet> results = downsampleTrackingService
          .retrieveReadyOnes(50).collectList().block();

      assertThat(results).isNotNull();
      assertThat(results).containsExactly(pending1);
    }

    @Test
    void notBusyOnes() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.000Z");

      final PendingDownsampleSet pending1 = createPending(timeSlot, 50, 1);
      final PendingDownsampleSet pending2 = createPending(timeSlot, 50, 120);

      cassandraTemplate.insert(pending1)
          .and(
              cassandraTemplate.insert(pending2)
          ).block();

      // current time is just after last touch of pending2, but within 2 minute delay period
      when(timestampProvider.now())
          .thenReturn(timeSlot.plus(121, ChronoUnit.MINUTES));

      final List<PendingDownsampleSet> results = downsampleTrackingService
          .retrieveReadyOnes(50).collectList().block();

      assertThat(results).isNotNull();
      assertThat(results).containsExactly(pending1);
    }
  }

  @Test
  void complete() {
    final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.000Z");

    final PendingDownsampleSet pending1 = createPending(timeSlot, 50, 1);
    final PendingDownsampleSet pending2 = createPending(timeSlot, 50, 120);

    cassandraTemplate.insert(pending1)
        .and(
            cassandraTemplate.insert(pending2)
        ).block();

    downsampleTrackingService.complete(pending1)
        .block();

    final List<PendingDownsampleSet> results = cassandraTemplate.select(
        Query.query(
            where("partition").is(50)
        ),
        PendingDownsampleSet.class
    ).collectList().block();

    assertThat(results).containsExactly(pending2);
  }

  private PendingDownsampleSet createPending(Instant timeSlot, int partition, int lastTouchOffsetMinutes) {
    return new PendingDownsampleSet()
        .setPartition(partition)
        .setTenant(RandomStringUtils.randomAlphanumeric(10))
        .setSeriesSet(
            RandomStringUtils.randomAlphabetic(5) + ",deployment=prod,host=h-1,os=linux")
        .setTimeSlot(timeSlot)
        .setLastTouch(timeSlot.plus(lastTouchOffsetMinutes, ChronoUnit.MINUTES));
  }

}
