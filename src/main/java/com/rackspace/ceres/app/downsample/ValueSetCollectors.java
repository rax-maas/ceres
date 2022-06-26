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

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collector;

@Slf4j
public class ValueSetCollectors {
  public static Collector<ValueSet, AggregatedValueSet, AggregatedValueSet> collector(
      Aggregator aggregator, Duration windowSize) {
    return switch (aggregator) {
      case min -> Collector.of(
          AggregatedValueSet::new,
          (agg, in) -> {
            DownsampledValueSet set = (DownsampledValueSet) in;
            agg.setTimestamp(minTimestamp(agg.getTimestamp(), in.getTimestamp()));
            agg.setMin(Double.min(agg.getMin(), set.getValue()));
            agg.setCount(agg.getCount() + set.getCount());
          },
          ValueSetCollectors::combineGauges,
          agg -> {
            agg.setTimestamp(agg.getTimestamp().with(new TemporalNormalizer(windowSize)));
            agg.setGranularity(windowSize);
            return agg;
          }
      );
      case max -> Collector.of(
          AggregatedValueSet::new,
          (agg, in) -> {
            DownsampledValueSet set = (DownsampledValueSet) in;
            agg.setTimestamp(minTimestamp(agg.getTimestamp(), in.getTimestamp()));
            agg.setMax(Double.max(agg.getMax(), set.getValue()));
            agg.setCount(agg.getCount() + set.getCount());
          },
          ValueSetCollectors::combineGauges,
          agg -> {
            agg.setTimestamp(agg.getTimestamp().with(new TemporalNormalizer(windowSize)));
            agg.setGranularity(windowSize);
            return agg;
          }
      );
      case sum -> Collector.of(
          AggregatedValueSet::new,
          (agg, in) -> {
            DownsampledValueSet set = (DownsampledValueSet) in;
            agg.setTimestamp(minTimestamp(agg.getTimestamp(), in.getTimestamp()));
            agg.setSum(agg.getSum() + set.getValue());
            agg.setCount(agg.getCount() + set.getCount());
          },
          ValueSetCollectors::combineGauges,
          agg -> {
            agg.setTimestamp(agg.getTimestamp().with(new TemporalNormalizer(windowSize)));
            agg.setAverage(agg.getCount() > 0 ? agg.getSum() / agg.getCount() : Double.NaN);
            agg.setGranularity(windowSize);
            return agg;
          }
      );
      case avg -> Collector.of(
          // Not used
          AggregatedValueSet::new, (agg, in) -> agg.setCount(0), ValueSetCollectors::combineGauges, agg -> agg);
      case raw -> Collector.of(
          AggregatedValueSet::new,
          (agg, in) -> {
            final SingleValueSet set = (SingleValueSet) in;
            agg.setTimestamp(minTimestamp(agg.getTimestamp(), set.getTimestamp()));
            agg.setMin(Double.min(agg.getMin(), set.getValue()));
            agg.setMax(Double.max(agg.getMax(), set.getValue()));
            agg.setSum(agg.getSum() + set.getValue());
            agg.setCount(agg.getCount() + 1);
          },
          ValueSetCollectors::combineGauges,
          agg -> {
            agg.setTimestamp(agg.getTimestamp().with(new TemporalNormalizer(windowSize)));
            agg.setAverage(agg.getCount() > 0 ? agg.getSum() / agg.getCount() : Double.NaN);
            agg.setGranularity(windowSize);
            return agg;
          }
      );
    };
  }

  private static AggregatedValueSet combineGauges(AggregatedValueSet agg, AggregatedValueSet in) {
    agg.setTimestamp(minTimestamp(agg.getTimestamp(), in.getTimestamp()));
    agg.setMin(Double.min(agg.getMin(), in.getMin()));
    agg.setMax(Double.max(agg.getMax(), in.getMax()));
    agg.setSum(Double.sum(agg.getSum(), in.getSum()));
    agg.setCount(Integer.sum(agg.getCount(), in.getCount()));
    return agg;
  }

  private static Instant minTimestamp(Instant lhs, Instant rhs) {
    if (lhs == null) {
      return rhs;
    } else if (rhs == null) {
      return lhs;
    } else {
      return lhs.compareTo(rhs) < 0 ? lhs : rhs;
    }
  }
}
