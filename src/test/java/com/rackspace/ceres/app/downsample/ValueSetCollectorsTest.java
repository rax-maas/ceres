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

package com.rackspace.ceres.app.downsample;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class ValueSetCollectorsTest {

//  @Test
//  void gaugeCollector() {
//    final AggregatedValueSet result = Stream.of(
//        new SingleValueSet().setValue(1.2).setTimestamp(Instant.parse("2007-12-03T10:15:31.01Z")),
//        new SingleValueSet().setValue(2.5).setTimestamp(Instant.parse("2007-12-03T10:16:23.02Z")),
//        new SingleValueSet().setValue(3.1).setTimestamp(Instant.parse("2007-12-03T10:17:12.03Z")),
//        new SingleValueSet().setValue(1.1).setTimestamp(Instant.parse("2007-12-03T10:18:56.04Z"))
//    )
//        .collect(ValueSetCollectors.gaugeCollector(Duration.ofMinutes(5)));
//
//    assertThat(result.getMin()).isEqualTo(1.1);
//    assertThat(result.getMax()).isEqualTo(3.1);
//    assertThat(result.getSum()).isEqualTo(7.9);
//    assertThat(result.getCount()).isEqualTo(4);
//    assertThat(result.getAverage()).isEqualTo(1.975);
//    assertThat(result.getTimestamp()).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
//    assertThat(result.getGranularity()).isEqualTo(Duration.ofMinutes(5));
//  }
}
