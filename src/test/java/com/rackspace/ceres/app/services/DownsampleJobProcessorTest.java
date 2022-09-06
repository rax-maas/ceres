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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.commons.lang3.RandomStringUtils;
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
    "ceres.downsample.partitions-to-process=0,1,3-5"
})
@ActiveProfiles(profiles = {"test", "downsample"})
@Testcontainers
public class DownsampleJobProcessorTest {

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
  private ScheduledExecutorService executor;

  @MockBean
  private DownsampleProcessor downsampleProcessor;

  @Autowired
  DownsampleJobProcessor downsampleJobProcessor;

  @Test
  public void testProcessTimeSlot() throws InterruptedException {
    Instant now = Instant.now();

    final String tenant = RandomStringUtils.randomAlphanumeric(10);
    final String seriesSet = RandomStringUtils.randomAlphanumeric(10) + ",host=h-1";
    final PendingDownsampleSet pendingSet = new PendingDownsampleSet()
        .setSeriesSetHash(seriesSet)
        .setTenant(tenant)
        .setTimeSlot(now);

    when(downsampleTrackingService.claimJob(any(), anyString()))
        .thenReturn(Flux.just("free"));
    when(downsampleTrackingService.getTimeSlot(any(), anyString()))
        .thenReturn(Mono.just(String.valueOf(now.getEpochSecond())));
    when(downsampleTrackingService.getDownsampleSets(anyLong(), anyInt()))
        .thenReturn(Flux.just(pendingSet));
    when(downsampleProcessor.processSet(any(PendingDownsampleSet.class), anyInt(), anyString(), anyString()))
        .thenReturn(Flux.empty());
    when(downsampleTrackingService.deleteTimeslot(anyInt(), anyString(), anyLong()))
        .thenReturn(Mono.empty());

    verify(downsampleTrackingService, atLeast(1)).freeJob(0, "PT15M");
    Thread.sleep(1*60*1000);
    verify(downsampleTrackingService, atLeast(1)).freeJob(0, "PT15M");
    verify(downsampleTrackingService, atLeast(1)).claimJob(0, "PT15M");
    verify(downsampleTrackingService, atLeast(1)).getTimeSlot(0, "PT15M");
  }
}
