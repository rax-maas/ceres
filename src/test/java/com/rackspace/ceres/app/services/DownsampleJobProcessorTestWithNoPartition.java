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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.config.DownsampleProperties;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SpringBootTest(properties = {
    "ceres.downsample.partitions=0"
})
@ActiveProfiles(profiles = {"test", "downsample"})
@Testcontainers
public class DownsampleJobProcessorTestWithNoPartition {

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
  ReactiveCqlTemplate cqlTemplate;

  @MockBean
  DownsampleTrackingService downsampleTrackingService;

  @Autowired
  ScheduledExecutorService executor;

  @MockBean
  DownsampleProcessor downsampleProcessor;

  @Autowired
  DownsampleJobProcessor downsampleJobProcessor;

  @Autowired
  DownsampleProperties downsampleProperties;

  @Test
  public void testDownsampleJobProcessorWithNoPartition() {
    Instant now = Instant.now();
    when(downsampleTrackingService.claimJob(any(), anyString()))
        .thenReturn(Flux.just("free"));
    when(downsampleTrackingService.getTimeSlot(any(), anyString()))
        .thenReturn(Mono.just(String.valueOf(now.getEpochSecond())));
    verifyNoInteractions(downsampleTrackingService);
  }
}
