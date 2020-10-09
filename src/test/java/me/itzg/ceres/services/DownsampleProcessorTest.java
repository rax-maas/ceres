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

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import me.itzg.ceres.config.DownsampleProperties;
import me.itzg.ceres.config.DownsampleProperties.Granularity;
import me.itzg.ceres.config.StringToIntegerSetConverter;
import me.itzg.ceres.downsample.AggregatedValueSet;
import me.itzg.ceres.downsample.Aggregator;
import me.itzg.ceres.downsample.DataDownsampled;
import me.itzg.ceres.downsample.SingleValueSet;
import me.itzg.ceres.downsample.ValueSet;
import me.itzg.ceres.services.DownsampleProcessorTest.TestConfig;
import org.junit.jupiter.api.Assertions;
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
import reactor.test.StepVerifier;
import reactor.util.function.Tuples;

@SpringBootTest(classes = {
    TestConfig.class,
    StringToIntegerSetConverter.class,
    DownsampleProcessor.class
}, properties = {
    "ceres.downsample.partitions-to-process=0,1,4-7"
})
@ActiveProfiles("test")
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
  SeriesSetService seriesSetService;

  @MockBean
  QueryService queryService;

  @MockBean
  DataWriteService dataWriteService;

  @MockBean
  MetadataService metadataService;

  @Autowired
  DownsampleProcessor downsampleProcessor;

  @Autowired
  DownsampleProperties downsampleProperties;

  @Captor
  ArgumentCaptor<Flux<DataDownsampled>> dataDownsampledCaptor;

  @Test
  void partitionsToProcessConfig() {
    assertThat(downsampleProperties.getPartitionsToProcess())
        .containsExactly(0, 1, 4, 5, 6, 7);
  }

  @Test
  void setupSchedulers() {
    Assertions.fail("TODO");
  }

  @Test
  void processDownsampleSet() {
    Assertions.fail("TODO");
  }

  @Test
  void aggregateSomeRawData() {
    when(dataWriteService.storeDownsampledData(any(), any()))
        .then(invocationOnMock -> {
          final Flux<DataDownsampled> data = invocationOnMock.getArgument(0);
          return data.map(dataDownsampled -> Tuples.of(dataDownsampled, true));
        });

    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10)+",host=h-1";

    StepVerifier.create(
        downsampleProcessor.aggregateData(
            Flux.just(
                singleValue("2007-12-03T10:01:23.00Z", 1.2),
                singleValue("2007-12-03T10:16:11.00Z", 1.5),
                singleValue("2007-12-03T10:31:21.00Z", 1.1),
                singleValue("2007-12-03T10:47:01.00Z", 3.4)
            ), tenant, seriesSet,
            List.of(granularity(15, 12), granularity(60, 24)).iterator(),
            false
        )
            // just count them since actual values verified within calls below
            .count()
    )
        .expectNext(20L)
        .verifyComplete();

    verify(dataWriteService).storeDownsampledData(dataDownsampledCaptor.capture(), eq(Duration.ofHours(12)));
    verify(dataWriteService).storeDownsampledData(dataDownsampledCaptor.capture(), eq(Duration.ofHours(24)));

    StepVerifier.create(
        dataDownsampledCaptor.getAllValues().get(0)
    )
        .expectNext(
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 15, Aggregator.sum, 1.2),
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 15, Aggregator.min, 1.2),
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 15, Aggregator.max, 1.2),
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 15, Aggregator.avg, 1.2)
        )
        .expectNext(
            dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15, Aggregator.sum, 1.5),
            dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15, Aggregator.min, 1.5),
            dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15, Aggregator.max, 1.5),
            dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15, Aggregator.avg, 1.5)
        )
        .expectNext(
            dataDownsampled("2007-12-03T10:30:00.00Z", tenant, seriesSet, 15, Aggregator.sum, 1.1),
            dataDownsampled("2007-12-03T10:30:00.00Z", tenant, seriesSet, 15, Aggregator.min, 1.1),
            dataDownsampled("2007-12-03T10:30:00.00Z", tenant, seriesSet, 15, Aggregator.max, 1.1),
            dataDownsampled("2007-12-03T10:30:00.00Z", tenant, seriesSet, 15, Aggregator.avg, 1.1)
        )
        .expectNext(
            dataDownsampled("2007-12-03T10:45:00.00Z", tenant, seriesSet, 15, Aggregator.sum, 3.4),
            dataDownsampled("2007-12-03T10:45:00.00Z", tenant, seriesSet, 15, Aggregator.min, 3.4),
            dataDownsampled("2007-12-03T10:45:00.00Z", tenant, seriesSet, 15, Aggregator.max, 3.4),
            dataDownsampled("2007-12-03T10:45:00.00Z", tenant, seriesSet, 15, Aggregator.avg, 3.4)
        )
        .verifyComplete();

    StepVerifier.create(
        dataDownsampledCaptor.getAllValues().get(1)
    )
        .expectNext(
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60, Aggregator.sum, 7.2),
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60, Aggregator.min, 1.1),
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60, Aggregator.max, 3.4),
            dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60, Aggregator.avg, 1.8)
        )
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  void aggregateOneRawData() {
    when(dataWriteService.storeDownsampledData(any(), any()))
        .then(invocationOnMock -> {
          final Flux<DataDownsampled> data = invocationOnMock.getArgument(0);
          return data.map(dataDownsampled -> Tuples.of(dataDownsampled, true));
        });

    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10)+",host=h-1";

    StepVerifier.create(
        downsampleProcessor.aggregateData(
            Flux.just(
                singleValue("2007-12-03T10:01:23.00Z", 1.2)
            ), tenant, seriesSet,
            List.of(granularity(1, 12), granularity(2, 24)).iterator(),
            false
        )
            // just count them since actual values verified within calls below
            .count()
    )
        .expectNext(8L)
        .verifyComplete();

    verify(dataWriteService).storeDownsampledData(dataDownsampledCaptor.capture(), eq(Duration.ofHours(12)));
    verify(dataWriteService).storeDownsampledData(dataDownsampledCaptor.capture(), eq(Duration.ofHours(24)));

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  void expandAggregatedData() {
    final String tenant = randomAlphanumeric(10);
    final String seriesSet = randomAlphanumeric(10)+",host=h-1";

    StepVerifier.create(
        downsampleProcessor.expandAggregatedData(
            Flux.just(
                aggregatedValueSet("2007-12-03T10:15:00.00Z", 15, 10, 1, 5, 2),
                aggregatedValueSet("2007-12-03T10:00:00.00Z", 60, 100, 10, 50, 20)
            ),
            tenant,
            seriesSet,
            false
        )
    )
        .expectNext(dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15,
            Aggregator.sum, 10
        ))
        .expectNext(dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15,
            Aggregator.min, 1
        ))
        .expectNext(dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15,
            Aggregator.max, 5
        ))
        .expectNext(dataDownsampled("2007-12-03T10:15:00.00Z", tenant, seriesSet, 15,
            Aggregator.avg, 2
        ))
        .expectNext(dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60,
            Aggregator.sum, 100
        ))
        .expectNext(dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60,
            Aggregator.min, 10
        ))
        .expectNext(dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60,
            Aggregator.max, 50
        ))
        .expectNext(dataDownsampled("2007-12-03T10:00:00.00Z", tenant, seriesSet, 60,
            Aggregator.avg, 20
        ))
        .verifyComplete();

    verifyNoMoreInteractions(seriesSetService, queryService);
  }

  private Granularity granularity(int minutes, int ttlHours) {
    return new Granularity()
        .setWidth(Duration.ofMinutes(minutes))
        .setTtl(Duration.ofHours(ttlHours));
  }

  private DataDownsampled dataDownsampled(String timestamp, String tenant, String seriesSet,
                                          int granularityMinutes, Aggregator aggregator,
                                          double value) {
    return new DataDownsampled()
        .setTs(Instant.parse(timestamp))
        .setTenant(tenant)
        .setSeriesSet(seriesSet)
        .setGranularity(Duration.ofMinutes(granularityMinutes))
        .setAggregator(aggregator)
        .setValue(value);
  }

  private AggregatedValueSet aggregatedValueSet(String timestamp, long granularityMinutes,
                                                double sum, double min, double max,
                                                double avg) {
    final AggregatedValueSet aggregatedValueSet = new AggregatedValueSet()
        .setGranularity(Duration.ofMinutes(granularityMinutes))
        .setMin(min)
        .setMax(max)
        .setSum(sum)
        .setAverage(avg);
    aggregatedValueSet
        .setTimestamp(Instant.parse(timestamp));
    return aggregatedValueSet;
  }

  private ValueSet singleValue(String timestamp, double value) {
    return new SingleValueSet()
        .setValue(value).setTimestamp(Instant.parse(timestamp));
  }

}
