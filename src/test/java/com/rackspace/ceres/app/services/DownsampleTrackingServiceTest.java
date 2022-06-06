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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.services.DownsampleTrackingServiceTest.RedisEnvInit;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import com.rackspace.ceres.app.utils.WebClientUtils;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
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
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

@SpringBootTest(properties = {
    "ceres.downsample.enabled=true",
    "ceres.downsample.partitions=64",
    "ceres.downsample.time-slot-width=PT2H",
    "ceres.downsample.last-touch-delay=PT2M"
}, classes = {
    RedisAutoConfiguration.class,
    RedisReactiveAutoConfiguration.class,
    DownsampleTrackingService.class,
    SeriesSetService.class,
    WebClientUtils.class,
    ObjectMapper.class
})
@EnableConfigurationProperties(DownsampleProperties.class)
@ContextConfiguration(initializers = RedisEnvInit.class)
@Testcontainers
@ActiveProfiles("test")
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

  @Autowired
  ObjectMapper objectMapper;

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

    final Instant timeslot = Instant.parse("2020-09-12T19:42:23.658447900Z");
    final Metric metric = new Metric()
        .setTimestamp(timeslot)
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
    List<String> widths = DateTimeUtils.getPartitionWidths(downsampleProperties.getGranularities());
    final Instant normalizedTimeSlotPt2h = timeslot.with(new TemporalNormalizer(Duration.parse(widths.get(0))));
    final Instant normalizedTimeSlotPt4h = timeslot.with(new TemporalNormalizer(Duration.parse(widths.get(1))));

    final String pendingKeyPt2h = "pending|36|" + widths.get(0);
    final String pendingKeyPt4h = "pending|36|" + widths.get(1);

    final String downsamplingKeyPt2h = "downsampling|36|"+ widths.get(0)+"|" +normalizedTimeSlotPt2h.getEpochSecond();
    final String downsamplingKeyPt4h = "downsampling|36|"+ widths.get(1)+"|" +normalizedTimeSlotPt4h.getEpochSecond();

    assertThat(keys).containsExactlyInAnyOrder(
        pendingKeyPt2h,
        pendingKeyPt4h,
        downsamplingKeyPt2h,
        downsamplingKeyPt4h
    );

    final List<String> pendingValuePt2h = redisTemplate.opsForSet().scan(pendingKeyPt2h).collectList().block();
    assertThat(pendingValuePt2h).containsExactlyInAnyOrder(
        Long.toString(normalizedTimeSlotPt2h.getEpochSecond())
    );
    final List<String> pendingValuePt4h = redisTemplate.opsForSet().scan(pendingKeyPt4h).collectList().block();
    assertThat(pendingValuePt4h).containsExactlyInAnyOrder(
        Long.toString(normalizedTimeSlotPt4h.getEpochSecond())
    );

    final List<String> downsamplingValuePt2h = redisTemplate.opsForSet().scan(downsamplingKeyPt2h).collectList().block();
    assertThat(downsamplingValuePt2h).containsExactlyInAnyOrder(
        tenantId+"|"+seriesSetHash
    );
    final List<String> downsamplingValuePt4h = redisTemplate.opsForSet().scan(downsamplingKeyPt4h).collectList().block();
    assertThat(downsamplingValuePt4h).containsExactlyInAnyOrder(
        tenantId+"|"+seriesSetHash
    );
  }

  @Nested
  class complete {
    @Test
    void oneRemains() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");

      final PendingDownsampleSet pending1 = createPending(timeSlot);
      final PendingDownsampleSet pending2 = createPending(timeSlot);

      final Instant normalizedTimeSlotPt2h = timeSlot.with(new TemporalNormalizer(Duration.parse("PT2H")));
      final String downsamplingKeyPt2h = "downsampling|36|"+ "PT2H"+"|" +normalizedTimeSlotPt2h.getEpochSecond();

      final String value1 = buildPendingValue(pending1);
      final String value2 = buildPendingValue(pending2);

      redisTemplate.opsForSet().add(downsamplingKeyPt2h, value1).block();
      redisTemplate.opsForSet().add(downsamplingKeyPt2h, value2).block();

      final List<String> before = redisTemplate.opsForSet().scan(downsamplingKeyPt2h).collectList().block();
      assertThat(before).containsExactlyInAnyOrder(
          value1, value2
      );

      downsampleTrackingService.complete(pending1,36, "PT2H" )
          .block();

      final List<String> after = redisTemplate.opsForSet().scan(downsamplingKeyPt2h).collectList().block();
      assertThat(after).containsExactlyInAnyOrder(
          value2
      );
    }

    @Test
    void noneRemain() {
      final Instant timeSlot = Instant.parse("2020-09-12T18:00:00.0Z");

      final PendingDownsampleSet pending1 = createPending(timeSlot);
      final Instant normalizedTimeSlotPt2h = timeSlot.with(new TemporalNormalizer(Duration.parse("PT2H")));
      final String downsamplingKeyPt2h = "downsampling|36|"+ "PT2H"+"|" +normalizedTimeSlotPt2h.getEpochSecond();

      final String value1 = buildPendingValue(pending1);
      final String pendingKeyPt2h = "pending|36|" + "PT2H";
      final String pendingValuePt2h = Long.toString(normalizedTimeSlotPt2h.getEpochSecond());

      redisTemplate.opsForSet().add(downsamplingKeyPt2h, value1).block();
      redisTemplate.opsForSet().add(pendingKeyPt2h, pendingValuePt2h).block();

      final List<String> before = redisTemplate.opsForSet().scan(downsamplingKeyPt2h).collectList().block();
      assertThat(before).containsExactlyInAnyOrder(
          value1
      );
      final List<String> beforePendingKey = redisTemplate.opsForSet().scan(pendingKeyPt2h).collectList().block();
      assertThat(beforePendingKey).containsExactlyInAnyOrder(
          pendingValuePt2h
      );

      downsampleTrackingService.complete(pending1, 36, "PT2H")
          .block();

      final List<String> after = redisTemplate.opsForSet().scan(downsamplingKeyPt2h).collectList().block();
      assertThat(after).isEmpty();
      final List<String> afterPendingKey = redisTemplate.opsForSet().scan(pendingKeyPt2h).collectList().block();
      assertThat(afterPendingKey).isEmpty();

      final Boolean hasKey = redisTemplate.hasKey(downsamplingKeyPt2h).block();
      assertThat(hasKey).isFalse();
    }

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
}
