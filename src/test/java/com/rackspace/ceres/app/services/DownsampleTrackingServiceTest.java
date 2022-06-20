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

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.services.DownsampleTrackingServiceTest.RedisEnvInit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisReactiveAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ContextConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest(properties = {
    "ceres.downsample.enabled=true",
    "ceres.downsample.partitions=4"
}, classes = {
    RedisAutoConfiguration.class,
    RedisReactiveAutoConfiguration.class,
    DownsampleTrackingService.class,
    SeriesSetService.class
})
@EnableConfigurationProperties({AppProperties.class, DownsampleProperties.class})
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

  @Autowired
  SeriesSetService seriesSetService;

  @MockBean
  ReactiveCassandraTemplate reactiveCassandraTemplate;

  @MockBean
  RedisScript<String> redisScript;

  @AfterEach
  void tearDown() {
    redisTemplate.delete(redisTemplate.scan()).block();
  }
//  @Test
//  void test() {
//  }
}
