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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TimeSlotPartitionerTest {

  AppProperties appProperties = new AppProperties()
      .setRawPartitionWidth(Duration.ofHours(1));
  DownsampleProperties downsampleProperties = new DownsampleProperties()
      .setGranularities(List.of(
          new Granularity()
              .setWidth(Duration.ofHours(1))
              .setPartitionWidth(Duration.ofHours(24))
      ));

  @Test
  void rawTimeSlot() {
    final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
        appProperties, downsampleProperties);

    final Instant result = partitioner.rawTimeSlot(
        Instant.parse("2007-12-03T10:15:30.00Z"));

    assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:00:00.00Z"));
  }

  @Nested
  public class downsampledTimeSlot {
    @Test
    void success() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      final Instant result = partitioner.downsampledTimeSlot(
          Instant.parse("2007-12-03T10:15:30.00Z"),
          Duration.ofHours(1)
      );

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T00:00:00.00Z"));
    }

    @Test
    void unknownGranularity() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      assertThatThrownBy(() -> {
        partitioner.downsampledTimeSlot(
            Instant.parse("2007-12-03T10:15:30.00Z"),
            Duration.ofMinutes(9)
        );
      })
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Nested
  public class partitionsOverRange {
    @Test
    void rawWithinOneSlot() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      final Iterable<Instant> results = partitioner.partitionsOverRange(
          Instant.parse("2007-12-03T10:15:30.00Z"),
          Instant.parse("2007-12-03T10:19:30.00Z"),
          null
      );

      assertThat(results)
          .containsExactlyInAnyOrder(Instant.parse("2007-12-03T10:00:00.00Z"));
    }

    @Test
    void rawExactlyOneSlot() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      final Iterable<Instant> results = partitioner.partitionsOverRange(
          Instant.parse("2007-12-03T10:00:00.00Z"),
          Instant.parse("2007-12-03T11:00:00.00Z"),
          null
      );

      assertThat(results)
          .containsExactlyInAnyOrder(Instant.parse("2007-12-03T10:00:00.00Z"));
    }

    @Test
    void rawSpanningTwo() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      final Iterable<Instant> results = partitioner.partitionsOverRange(
          Instant.parse("2007-12-03T10:15:30.00Z"),
          Instant.parse("2007-12-03T11:19:30.00Z"),
          null
      );

      assertThat(results)
          .containsExactlyInAnyOrder(
              Instant.parse("2007-12-03T10:00:00.00Z"),
              Instant.parse("2007-12-03T11:00:00.00Z")
          );
    }

    @Test
    void downsampleSpotCheck() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      final Iterable<Instant> results = partitioner.partitionsOverRange(
          Instant.parse("2007-12-02T10:15:30.00Z"),
          Instant.parse("2007-12-04T11:19:30.00Z"),
          Duration.ofHours(1)
      );

      assertThat(results)
          .containsExactlyInAnyOrder(
              Instant.parse("2007-12-02T00:00:00.00Z"),
              Instant.parse("2007-12-03T00:00:00.00Z"),
              Instant.parse("2007-12-04T00:00:00.00Z")
          );
    }

    @Test
    void unknownDownsampleGranularity() {
      final TimeSlotPartitioner partitioner = new TimeSlotPartitioner(
          appProperties, downsampleProperties);

      assertThatThrownBy(() -> {
        partitioner.partitionsOverRange(
            Instant.parse("2007-12-02T10:15:30.00Z"),
            Instant.parse("2007-12-04T11:19:30.00Z"),
            Duration.ofMinutes(7)
        );
      })
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

}