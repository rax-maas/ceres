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
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.config.StringToIntegerSetConverter;
import com.rackspace.ceres.app.downsample.*;
import com.rackspace.ceres.app.services.DownsampleProcessorTest.TestConfig;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.task.TaskSchedulerBuilder;
import org.springframework.boot.test.autoconfigure.json.AutoConfigureJson;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.test.context.ActiveProfiles;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = {
    TestConfig.class,
    StringToIntegerSetConverter.class,
    DownsampleProcessor.class
}, properties = {
    "ceres.downsample.partitions-to-process=0,1,3-5"
})
@ActiveProfiles(profiles = {"test", "downsample"})
@AutoConfigureJson
@EnableConfigurationProperties(DownsampleProperties.class)
class DownsampleProcessorTest {

  @TestConfiguration
  static class TestConfig {
    @Bean
    public TaskScheduler downsampleTaskScheduler() {
      return new TaskSchedulerBuilder().build();
    }
  }

  @MockBean
  DownsampleTrackingService downsampleTrackingService;

  @MockBean
  DelayedTrackingService delayedTrackingService;

  @MockBean
  SeriesSetService seriesSetService;

  @MockBean
  QueryService queryService;

  @MockBean
  DataWriteService dataWriteService;

  @MockBean
  MetadataService metadataService;

  @MockBean
  MeterRegistry meterRegistry;

  @MockBean
  ScheduledExecutorService executorService;

  @Autowired
  DownsampleProcessor downsampleProcessor;

  @Autowired
  DownsampleProperties downsampleProperties;

  @Captor
  ArgumentCaptor<Flux<DataDownsampled>> dataDownsampledCaptor;

  @Test
  void aggregateSomeRawData() {
    when(dataWriteService.storeDownsampledData(any()))
        .thenReturn(Mono.empty());

    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10) + ",host=h-1";

    StepVerifier.create(downsampleProcessor.downsampleData(
                Flux.just(
                    singleValue("2007-12-03T10:01:23.00Z", 1.2),
                    singleValue("2007-12-03T10:16:11.00Z", 1.5),
                    singleValue("2007-12-03T10:31:21.00Z", 1.1),
                    singleValue("2007-12-03T10:47:01.00Z", 3.4)
                ), tenant, seriesSet,
                List.of(granularity(15, 12), granularity(60, 24)).iterator()
            )
        )
        .verifyComplete();

    verify(dataWriteService, times(2)).storeDownsampledData(dataDownsampledCaptor.capture());

    StepVerifier.create(dataDownsampledCaptor.getAllValues().get(0))
        .expectNext(
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 15, 1.2, 1.2, 1.2, 1.2, 1)
        )
        .expectNext(
            dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15, 1.5, 1.5, 1.5, 1.5, 1)
        )
        .expectNext(
            dataDownsampled("2007-12-03T10:30:00.00Z", tenant, seriesSet, 15, 1.1, 1.1, 1.1, 1.1, 1)
        )
        .expectNext(
            dataDownsampled("2007-12-03T10:45:00.00Z", tenant, seriesSet, 15, 3.4, 3.4, 3.4, 3.4, 1)
        )
        .verifyComplete();

    StepVerifier.create(dataDownsampledCaptor.getAllValues().get(1))
        .expectNext(
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60, 1.1, 3.4, 7.2, 1.8, 4)
        )
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  void aggregateOneRawData() {
    when(dataWriteService.storeDownsampledData(any()))
        .thenReturn(Mono.empty());

    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10) + ",host=h-1";

    StepVerifier.create(downsampleProcessor.downsampleData(
        Flux.just(singleValue("2007-12-03T10:01:23.00Z", 1.2)), tenant, seriesSet,
                List.of(granularity(1, 12), granularity(2, 24)).iterator()))
        .verifyComplete();

    verify(dataWriteService, times(2)).storeDownsampledData(dataDownsampledCaptor.capture());

    assertThat(dataDownsampledCaptor.getAllValues().get(0).toIterable())
        .containsExactly(
            // normalized to the 1-minute slot at 10:01
            // all values match since aggregate of single value is the value
            dataDownsampled("2007-12-03T10:01:00.00Z", tenant, seriesSet, 1, 1.2, 1.2, 1.2, 1.2, 1)
        );
    assertThat(dataDownsampledCaptor.getAllValues().get(1).toIterable())
        .containsExactly(
            // same values, just normalized down to the 2-minute slot at 10:00
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 2, 1.2, 1.2, 1.2, 1.2, 1)
        );

    verifyNoMoreInteractions(dataWriteService);
  }

  private Granularity granularity(int minutes, int ttlHours) {
    return new Granularity()
        .setWidth(Duration.ofMinutes(minutes))
        .setTtl(Duration.ofHours(ttlHours));
  }

  private DataDownsampled dataDownsampled(String timestamp, String tenant, String seriesSet,
                                          int gMin, double min, double max, double sum, double avg, int count) {
    return new DataDownsampled()
        .setTs(Instant.parse(timestamp))
        .setTenant(tenant)
        .setSeriesSetHash(seriesSet)
        .setGranularity(Duration.ofMinutes(gMin))
        .setMin(min)
        .setMax(max)
        .setSum(sum)
        .setAvg(avg)
        .setCount(count);
  }

  private ValueSet singleValue(String timestamp, double value) {
    return new SingleValueSet()
        .setValue(value).setTimestamp(Instant.parse(timestamp));
  }
}
