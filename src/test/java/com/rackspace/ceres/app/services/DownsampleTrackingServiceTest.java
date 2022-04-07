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

package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.RedisConfig;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.services.DownsampleTrackingServiceTest.RedisEnvInit;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(properties = {
    "ceres.downsample.enabled=true",
    "ceres.downsample.partitions=64",
    "ceres.downsample.time-slot-width=PT2H",
    "ceres.downsample.last-touch-delay=PT2M"
}, classes = {
    RedisAutoConfiguration.class,
    RedisReactiveAutoConfiguration.class,
    DownsampleTrackingService.class,
    RedisConfig.class
})
@EnableConfigurationProperties(DownsampleProperties.class)
@ContextConfiguration(initializers = RedisEnvInit.class)
@Testcontainers
class DownsampleTrackingServiceTest {

  private static final int REDIS_PORT = 6379;

  @Container
  public static GenericContainer<?> redisContainer =
      new GenericContainer<>(DockerImageName.parse( "redis:6.0"))
          .withExposedPorts(REDIS_PORT);

  public static class RedisEnvInit implements
      ApplicationContextInitializer<ConfigurableApplicationContext> {

    @Override
    public void initialize(ConfigurableApplicationContext ctx) {
      TestPropertyValues.of(
          "spring.redis.host=" + redisContainer.getHost(),
          "spring.redis.port=" + redisContainer.getMappedPort(REDIS_PORT)
      ).applyTo(ctx.getEnvironment());
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
    // need stable values to keep partition result stable for assertion
    final String tenantId = "tenant-1";
    final String seriesSetHash = "some_metric,some_tags";
    final String metricName = "some_metric";
    final Map<String, String> tags = Map.of(
        "os", "linux",
        "host", "h-1",
        "deployment", "prod"
    );

    final Instant normalizedTimeSlot = Instant.parse("2020-09-12T18:00:00.0Z");
    final Metric metric = new Metric()
        .setTimestamp(Instant.parse("2020-09-12T19:42:23.658447900Z"))
        .setValue(Math.random())
        .setMetric(metricName)
        .setTags(tags);
    Mono.from(
        downsampleTrackingService.track(
            tenantId,
            seriesSetHash, metric.getTimestamp()
        )
    ).block();

    final List<String> keys = redisTemplate.scan().collectList().block();

    final String ingestingKey = "ingesting|61|" + normalizedTimeSlot.getEpochSecond();
    final String pendingKey = "pending|61|" + normalizedTimeSlot.getEpochSecond();
    assertThat(keys).containsExactlyInAnyOrder(
        pendingKey,
        ingestingKey
    );

    final Duration expiration = redisTemplate.getExpire(ingestingKey).block();
    assertThat(expiration).isPositive();

    final List<String> pending = redisTemplate.opsForSet().scan(pendingKey).collectList().block();
    assertThat(pending).containsExactlyInAnyOrder(
        tenantId + "|" + seriesSetHash
    );
  }

  @Test
  void checkPartitionJobsLuaScript() {
    String now = isoTimeUtcPlusSeconds(0);
    String later = isoTimeUtcPlusSeconds(1);
    String next = isoTimeUtcPlusSeconds(5);

    setJobValue(1, later);

    // now is before later, so it's not time to run yet
    downsampleTrackingService.checkPartitionJobs(1, now, next)
        .flatMap(result -> {
          // Not time to execute job yet
          assertThat(result).isFalse();
          return Mono.empty();
        }).blockFirst();

    setJobValue(1, now);

    // now is the time to run the job
    downsampleTrackingService.checkPartitionJobs(1, now, next)
        .flatMap(result -> {
          // Time to run job
          assertThat(result).isTrue();
          return Mono.empty();
        }).blockFirst();

    setJobValue(1, now);

    // now is passed by later so we need to run the job
    downsampleTrackingService.checkPartitionJobs(1, later, next)
        .flatMap(result -> {
          // Time to run job
          assertThat(result).isTrue();
          return Mono.empty();
        }).blockFirst();

    // Check that the script set the value to next
    downsampleTrackingService.getJobValue(1).flatMap(
        result -> {
          assertThat(result).isEqualTo(next);
          return Mono.empty();
        }
    ).block();

    // ...and that if we compare to next it's time to run the job
    downsampleTrackingService.checkPartitionJobs(1, next, next)
        .flatMap(result -> {
          // Time to run job
          assertThat(result).isTrue();
          return Mono.empty();
        }).blockFirst();
  }

  @Nested
  class retrieveReadyOnes {

//    @Test
//    void onlyIncludesRequestedPartition() {
//      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");
//
//      final PendingDownsampleSet expected1 = createPending(50, timeSlot);
//      final PendingDownsampleSet expected2 = createPending(50, timeSlot);
//      final PendingDownsampleSet extra1 = createPending(25, timeSlot);
//
//      redisTemplate.opsForSet()
//          .add(
//              "pending|50|" + timeSlot.getEpochSecond(),
//              buildPendingValue(expected1),
//              buildPendingValue(expected2)
//          )
//          .block();
//      redisTemplate.opsForSet()
//          .add(
//              "pending|25|" + timeSlot.getEpochSecond(),
//              buildPendingValue(extra1)
//          )
//          .block();
//
//      final List<PendingDownsampleSet> results = downsampleTrackingService.retrieveReadyOnes(50)
//          .collectList().block();
//
//      assertThat(results).containsExactlyInAnyOrder(
//          expected1, expected2
//      );
//    }

//    @Test
//    void onlyOnesFinishedIngesting() {
//      final Instant timeSlotFinished = Instant.parse("2020-09-12T18:00:00.0Z");
//      final Instant timeSlotIngesting = Instant.parse("2020-09-12T20:00:00.0Z");
//
//      final PendingDownsampleSet expected1 = createPending(50, timeSlotFinished);
//      final PendingDownsampleSet extra1 = createPending(50, timeSlotIngesting);
//
//      redisTemplate.opsForSet()
//          .add(
//              "pending|50|" + timeSlotFinished.getEpochSecond(),
//              buildPendingValue(expected1)
//          )
//          .block();
//      redisTemplate.opsForSet()
//          .add(
//              "pending|50|" + timeSlotIngesting.getEpochSecond(),
//              buildPendingValue(extra1)
//          )
//          .block();
//      redisTemplate.opsForValue()
//          .set("ingesting|50|" + timeSlotIngesting.getEpochSecond(), "")
//          .block();
//
//      final List<PendingDownsampleSet> results = downsampleTrackingService.retrieveReadyOnes(50)
//          .collectList().block();
//
//      assertThat(results).containsExactlyInAnyOrder(
//          expected1
//      );
//    }

//    @Test
//    void multipleTimeSlots() {
//      final Instant timeSlot1 = Instant.parse("2020-09-12T18:00:00.0Z");
//      final Instant timeSlot2 = Instant.parse("2020-09-12T20:00:00.0Z");
//
//      final PendingDownsampleSet expected1 = createPending(50, timeSlot1);
//      final PendingDownsampleSet expected2 = createPending(50, timeSlot2);
//
//      redisTemplate.opsForSet()
//          .add(
//              "pending|50|" + timeSlot1.getEpochSecond(),
//              buildPendingValue(expected1)
//          )
//          .block();
//      redisTemplate.opsForSet()
//          .add(
//              "pending|50|" + timeSlot2.getEpochSecond(),
//              buildPendingValue(expected2)
//          )
//          .block();
//
//      final List<PendingDownsampleSet> results = downsampleTrackingService.retrieveReadyOnes(50)
//          .collectList().block();
//
//      assertThat(results).containsExactlyInAnyOrder(
//          expected1,
//          expected2
//      );
//    }
  }

  @Nested
  class complete {
//    @Test
//    void oneRemains() {
//      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");
//
//      final PendingDownsampleSet pending1 = createPending(50, timeSlot);
//      final PendingDownsampleSet pending2 = createPending(50, timeSlot);
//
//      final String key = "pending|50|" + timeSlot.getEpochSecond();
//
//      final String value1 = buildPendingValue(pending1);
//      final String value2 = buildPendingValue(pending2);
//
//      redisTemplate.opsForSet().add(key, value1).block();
//      redisTemplate.opsForSet().add(key, value2).block();
//
//      final List<String> before = redisTemplate.opsForSet().scan(key).collectList().block();
//      assertThat(before).containsExactlyInAnyOrder(
//          value1, value2
//      );
//
//      downsampleTrackingService.complete(pending1)
//          .block();
//
//      final List<String> after = redisTemplate.opsForSet().scan(key).collectList().block();
//      assertThat(after).containsExactlyInAnyOrder(
//          value2
//      );
//    }

//    @Test
//    void noneRemain() {
//      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");
//
//      final PendingDownsampleSet pending1 = createPending(50, timeSlot);
//
//      final String key = "pending|50|" + timeSlot.getEpochSecond();
//
//      final String value1 = buildPendingValue(pending1);
//
//      redisTemplate.opsForSet().add(key, value1).block();
//
//      final List<String> before = redisTemplate.opsForSet().scan(key).collectList().block();
//      assertThat(before).containsExactlyInAnyOrder(
//          value1
//      );
//
//      downsampleTrackingService.complete(pending1)
//          .block();
//
//      final List<String> after = redisTemplate.opsForSet().scan(key).collectList().block();
//      assertThat(after).isEmpty();
//
//      final Boolean hasKey = redisTemplate.hasKey(key).block();
//      assertThat(hasKey).isFalse();
//    }

  }

  private PendingDownsampleSet createPending(Instant timeSlot) {
    return new PendingDownsampleSet()
        .setTenant(RandomStringUtils.randomAlphanumeric(10))
        .setSeriesSetHash(
            RandomStringUtils.randomAlphabetic(5) + ",deployment=prod,host=h-1,os=linux")
        .setTimeSlot(timeSlot);
  }

  private String buildPendingValue(PendingDownsampleSet pending) {
    return pending.getTenant() + "|" + pending.getSeriesSetHash();
  }

  private String isoTimeUtcPlusSeconds(long seconds) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    Date now = new Date();
    now.setTime(now.getTime() + seconds * 1000);
    return df.format(now);
  }

  private void setJobValue(int jobKey, String jobValue) {
    downsampleTrackingService.setJobValue(jobKey, jobValue).block();
    downsampleTrackingService.getJobValue(jobKey).flatMap(
        result -> {
          assertThat(result).isEqualTo(jobValue);
          return Mono.empty();
        }
    ).block();
  }
}
