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

package com.rackspace.monplat.ceres.downsample;

import java.time.Duration;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import lombok.Getter;

public class TemporalNormalizer implements TemporalAdjuster {

  @Getter
  final Duration rounding;

  public TemporalNormalizer(Duration rounding) {
    this.rounding = rounding;
  }

  @Override
  public Temporal adjustInto(Temporal temporal) {
    return temporal
        .with(ChronoField.NANO_OF_SECOND, 0)
        .with(ChronoField.INSTANT_SECONDS, round(temporal.getLong(ChronoField.INSTANT_SECONDS)));
  }

  private long round(long seconds) {
    return seconds - (seconds % rounding.getSeconds());
  }
}
