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
import com.rackspace.ceres.app.helper.MetricDeletionHelper;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
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
  MetricDeletionHelper metricDeletionHelper;

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
  ArgumentCaptor<Flux<AggregatedValueSet>> dataDownsampledCaptor;

  @Test
  void aggregateSomeRawData() {
    when(dataWriteService.storeDownsampledData(any(), any(), any())).thenReturn(Mono.empty());
    when(queryService.queryRawWithSeriesSet(any(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(List.of(
            singleValue("2007-12-03T10:01:23.00Z", 1.0),
            singleValue("2007-12-03T10:04:23.00Z", 1.2),
            singleValue("2007-12-03T10:14:23.00Z", 1.4)))
        );

    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10) + ",host=h-1";

    StepVerifier.create(
        downsampleProcessor.downsampleData(
            new PendingDownsampleSet()
                .setSeriesSetHash(seriesSet).setTenant(tenant).setTimeSlot(Instant.parse("2007-12-03T10:00:00.00Z")),
            "PT15M",
            granularity(15, 12))
    ).verifyComplete();

    verify(dataWriteService, times(1))
        .storeDownsampledData(dataDownsampledCaptor.capture(), any(), any());

    StepVerifier.create(dataDownsampledCaptor.getAllValues().get(0))
        .expectNext(
            aggregatedValueSet("2007-12-03T10:00:00.00Z", 15, 3.6, 1.0, 1.4, 1.2, 3)
        )
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  void aggregateOverTwoTimeslots() {
    when(dataWriteService.storeDownsampledData(any(), any(), any())).thenReturn(Mono.empty());
    when(queryService.queryRawWithSeriesSet(any(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(List.of(
            singleValue("2007-12-03T10:01:23.00Z", 1.0),
            singleValue("2007-12-03T10:04:23.00Z", 1.2),
            singleValue("2007-12-03T10:14:23.00Z", 1.4),
            singleValue("2007-12-03T10:16:23.00Z", 1.6)))
        );

    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10) + ",host=h-1";

    StepVerifier.create(
        downsampleProcessor.downsampleData(
            new PendingDownsampleSet()
                .setSeriesSetHash(seriesSet).setTenant(tenant).setTimeSlot(Instant.parse("2007-12-03T10:00:00.00Z")),
            "PT30M",
            granularity(15, 12))
    ).verifyComplete();

    verify(dataWriteService, times(1))
        .storeDownsampledData(dataDownsampledCaptor.capture(), any(), any());

    StepVerifier.create(dataDownsampledCaptor.getAllValues().get(0))
        .expectNext(
            aggregatedValueSet("2007-12-03T10:00:00.00Z", 15, 3.6, 1.0, 1.4, 1.2, 3)
        )
        .expectNext(
            aggregatedValueSet("2007-12-03T10:15:00.00Z", 15, 1.6, 1.6, 1.6, 1.6, 1)
        )
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }

  private Granularity granularity(int minutes, int ttlHours) {
    return new Granularity()
        .setWidth(Duration.ofMinutes(minutes))
        .setTtl(Duration.ofHours(ttlHours));
  }

  private AggregatedValueSet aggregatedValueSet(String timestamp, long granularityMinutes,
                                                double sum, double min, double max,
                                                double avg, int count) {
    final AggregatedValueSet aggregatedValueSet = new AggregatedValueSet()
        .setGranularity(Duration.ofMinutes(granularityMinutes))
        .setMin(min)
        .setMax(max)
        .setSum(sum)
        .setAverage(avg)
        .setCount(count);
    aggregatedValueSet
        .setTimestamp(Instant.parse(timestamp));
    return aggregatedValueSet;
  }

  private ValueSet singleValue(String timestamp, double value) {
    return new SingleValueSet().setValue(value).setTimestamp(Instant.parse(timestamp));
  }

}
