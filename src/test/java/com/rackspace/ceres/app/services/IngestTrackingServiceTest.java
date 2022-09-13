/*
 * Copyright 2022 Rackspace US, Inc.
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
 *
 */

package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.entities.Downsampling;
import java.time.Instant;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Mono;

@SpringBootTest
@ActiveProfiles(profiles = {"test", "downsample"})
@Testcontainers
public class IngestTrackingServiceTest {

  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>(
      CassandraContainerSetup.DOCKER_IMAGE);
  @TestConfiguration
  @Import(CassandraContainerSetup.class)
  public static class TestConfig {
    @Bean
    CassandraContainer<?> cassandraContainer() {
      return cassandraContainer;
    }
  }

  @Autowired
  private DownsampleProperties properties;

  @Autowired
  private AppProperties appProperties;

  @Autowired
  private SeriesSetService hashService;

  @Autowired
  ReactiveCassandraTemplate cassandraTemplate;

  @MockBean
  private ReactiveStringRedisTemplate redisTemplate;

  private static final String QUERY_DOWNSAMPLING = "SELECT * FROM downsampling_hashes WHERE partition = ?"
      + " AND hash = ?";

  @Test
  public void testTrack() {
    IngestTrackingService ingestTrackingService = new IngestTrackingService(redisTemplate, cassandraTemplate,
        properties, hashService, appProperties, null, null, null,
        null);
    String tenant = RandomStringUtils.randomAlphanumeric(10);
    String seriesSetHash = RandomStringUtils.randomAlphanumeric(10);
    Instant instant = Instant.now();

    String setHash = ingestTrackingService.encodeSetHash(tenant, seriesSetHash);
    ingestTrackingService.track(tenant, seriesSetHash, instant);

    String query = String.format(QUERY_DOWNSAMPLING, 1, setHash);
    Mono<Downsampling> downsamplingMono = cassandraTemplate.selectOne(query, Downsampling.class);
    Assertions.assertThat(downsamplingMono.hasElement());
  }
}

