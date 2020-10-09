/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.itzg.ceres.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import me.itzg.ceres.config.DownsampleProperties;
import me.itzg.ceres.model.Metric;
import me.itzg.ceres.model.PendingDownsampleSet;
import me.itzg.ceres.services.DownsampleTrackingServiceTest.TestConfig;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@SpringBootTest(properties = {
    "app.downsample.enabled=true",
    "app.downsample.partitions=64",
    "app.downsample.time-slot-width=PT2H",
    "app.downsample.last-touch-delay=PT2M",
    "logging.level.cql=debug"
}, classes = {
    TestConfig.class,
    DownsampleTrackingService.class
})
@EnableConfigurationProperties(DownsampleProperties.class)
@ActiveProfiles("test")
@Testcontainers
class DownsampleTrackingServiceTest {

  private static final int REDIS_PORT = 6379;

  @Container
  public static GenericContainer<?> redisContainer =
      new GenericContainer<>("redis:6.0")
          .withExposedPorts(REDIS_PORT);

  @TestConfiguration
  public static class TestConfig {

    @Bean
    ReactiveRedisConnectionFactory redisConnectionFactory() {
      return new LettuceConnectionFactory(
          redisContainer.getHost(),
          redisContainer.getFirstMappedPort()
      );
    }

    @Bean
    ReactiveStringRedisTemplate reactiveStringRedisTemplate(
        ReactiveRedisConnectionFactory connectionFactory) {
      return new ReactiveStringRedisTemplate(connectionFactory);
    }
  }

  @Autowired
  DownsampleTrackingService downsampleTrackingService;

  @Autowired
  ReactiveStringRedisTemplate redisTemplate;

  @Autowired
  DownsampleProperties downsampleProperties;

  @AfterEach
  void tearDown() {
    // prune between tests
    redisTemplate.delete(redisTemplate.scan())
        .block();
  }

  @Test
  void track() {
    // need stable value to keep partition result stable for assertion
    final String tenantId = "tenant-1";
    final String metricName = "some_metric";
    final String seriesSet = metricName + ",deployment=prod,host=h-1,os=linux";

    final Instant normalizedTimeSlot = Instant.parse("2020-09-12T18:00:00.0Z");
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

    final List<String> keys = redisTemplate.scan().collectList().block();

    final String ingestingKey = "ingesting|50|" + normalizedTimeSlot.getEpochSecond();
    final String pendingKey = "pending|50|" + normalizedTimeSlot.getEpochSecond();
    assertThat(keys).containsExactlyInAnyOrder(
        pendingKey,
        ingestingKey
    );

    final Duration expiration = redisTemplate.getExpire(ingestingKey).block();
    assertThat(expiration).isPositive();

    final List<String> pending = redisTemplate.opsForSet().scan(pendingKey).collectList().block();
    assertThat(pending).containsExactlyInAnyOrder(
        tenantId + "|" + seriesSet
    );
  }

  @Nested
  class retrieveReadyOnes {

    @Test
    void onlyIncludesRequestedPartition() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");

      final PendingDownsampleSet expected1 = createPending(50, timeSlot);
      final PendingDownsampleSet expected2 = createPending(50, timeSlot);
      final PendingDownsampleSet extra1 = createPending(25, timeSlot);

      redisTemplate.opsForSet()
          .add(
              "pending|50|" + timeSlot.getEpochSecond(),
              buildPendingValue(expected1),
              buildPendingValue(expected2)
          )
          .block();
      redisTemplate.opsForSet()
          .add(
              "pending|25|" + timeSlot.getEpochSecond(),
              buildPendingValue(extra1)
          )
          .block();

      final List<PendingDownsampleSet> results = downsampleTrackingService.retrieveReadyOnes(50)
          .collectList().block();

      assertThat(results).containsExactlyInAnyOrder(
          expected1, expected2
      );
    }

    @Test
    void onlyOnesFinishedIngesting() {
      final Instant timeSlotFinished = Instant.parse("2020-09-12T18:00:00.0Z");
      final Instant timeSlotIngesting = Instant.parse("2020-09-12T20:00:00.0Z");

      final PendingDownsampleSet expected1 = createPending(50, timeSlotFinished);
      final PendingDownsampleSet extra1 = createPending(50, timeSlotIngesting);

      redisTemplate.opsForSet()
          .add(
              "pending|50|" + timeSlotFinished.getEpochSecond(),
              buildPendingValue(expected1)
          )
          .block();
      redisTemplate.opsForSet()
          .add(
              "pending|50|" + timeSlotIngesting.getEpochSecond(),
              buildPendingValue(extra1)
          )
          .block();
      redisTemplate.opsForValue()
          .set("ingesting|50|" + timeSlotIngesting.getEpochSecond(), "")
          .block();

      final List<PendingDownsampleSet> results = downsampleTrackingService.retrieveReadyOnes(50)
          .collectList().block();

      assertThat(results).containsExactlyInAnyOrder(
          expected1
      );
    }

    @Test
    void multipleTimeSlots() {
      final Instant timeSlot1 = Instant.parse("2020-09-12T18:00:00.0Z");
      final Instant timeSlot2 = Instant.parse("2020-09-12T20:00:00.0Z");

      final PendingDownsampleSet expected1 = createPending(50, timeSlot1);
      final PendingDownsampleSet expected2 = createPending(50, timeSlot2);

      redisTemplate.opsForSet()
          .add(
              "pending|50|" + timeSlot1.getEpochSecond(),
              buildPendingValue(expected1)
          )
          .block();
      redisTemplate.opsForSet()
          .add(
              "pending|50|" + timeSlot2.getEpochSecond(),
              buildPendingValue(expected2)
          )
          .block();

      final List<PendingDownsampleSet> results = downsampleTrackingService.retrieveReadyOnes(50)
          .collectList().block();

      assertThat(results).containsExactlyInAnyOrder(
          expected1,
          expected2
      );
    }
  }

  @Nested
  class complete {
    @Test
    void oneRemains() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");

      final PendingDownsampleSet pending1 = createPending(50, timeSlot);
      final PendingDownsampleSet pending2 = createPending(50, timeSlot);

      final String key = "pending|50|" + timeSlot.getEpochSecond();

      final String value1 = buildPendingValue(pending1);
      final String value2 = buildPendingValue(pending2);

      redisTemplate.opsForSet().add(key, value1).block();
      redisTemplate.opsForSet().add(key, value2).block();

      final List<String> before = redisTemplate.opsForSet().scan(key).collectList().block();
      assertThat(before).containsExactlyInAnyOrder(
          value1, value2
      );

      downsampleTrackingService.complete(pending1)
          .block();

      final List<String> after = redisTemplate.opsForSet().scan(key).collectList().block();
      assertThat(after).containsExactlyInAnyOrder(
          value2
      );
    }

    @Test
    void noneRemain() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");

      final PendingDownsampleSet pending1 = createPending(50, timeSlot);

      final String key = "pending|50|" + timeSlot.getEpochSecond();

      final String value1 = buildPendingValue(pending1);

      redisTemplate.opsForSet().add(key, value1).block();

      final List<String> before = redisTemplate.opsForSet().scan(key).collectList().block();
      assertThat(before).containsExactlyInAnyOrder(
          value1
      );

      downsampleTrackingService.complete(pending1)
          .block();

      final List<String> after = redisTemplate.opsForSet().scan(key).collectList().block();
      assertThat(after).isEmpty();

      final Boolean hasKey = redisTemplate.hasKey(key).block();
      assertThat(hasKey).isFalse();
    }

  }

  private PendingDownsampleSet createPending(int partition, Instant timeSlot) {
    return new PendingDownsampleSet()
        .setPartition(partition)
        .setTenant(RandomStringUtils.randomAlphanumeric(10))
        .setSeriesSet(
            RandomStringUtils.randomAlphabetic(5) + ",deployment=prod,host=h-1,os=linux")
        .setTimeSlot(timeSlot);
  }

  private String buildPendingValue(PendingDownsampleSet pending) {
    return pending.getTenant() + "|" + pending.getSeriesSet();
  }

}
