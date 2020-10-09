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

package me.itzg.ceres.downsample;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TemporalNormalizerTest {

  @Nested
  class roundTo5m {
    @Test
    void withinTheMinute() {
      final Instant result = Instant.parse("2007-12-03T10:15:32.08Z")
          .with(new TemporalNormalizer(Duration.ofMinutes(5)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    }

    @Test
    void outsideTheMinute() {
      final Instant result = Instant.parse("2007-12-03T10:17:23.45Z")
          .with(new TemporalNormalizer(Duration.ofMinutes(5)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    }

    @Test
    void alreadyAligned() {
      final Instant result = Instant.parse("2007-12-03T10:15:00.00Z")
          .with(new TemporalNormalizer(Duration.ofMinutes(5)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    }
  }

  @Nested
  class roundTo1h {

    @Test
    void normal() {
      final Instant result = Instant.parse("2007-12-03T10:15:32.08Z")
          .with(new TemporalNormalizer(Duration.ofHours(1)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:00:00.00Z"));
    }

    @Test
    void alreadyAligned() {
      final Instant result = Instant.parse("2007-12-03T10:00:00.00Z")
          .with(new TemporalNormalizer(Duration.ofHours(1)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:00:00.00Z"));
    }
  }
}
