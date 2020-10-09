package me.itzg.ceres.downsample;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collector;

public class ValueSetCollectors {

  public static Collector<ValueSet, AggregatedValueSet, AggregatedValueSet> gaugeCollector(Duration windowSize) {
    return Collector.of(
        AggregatedValueSet::new,
        (agg, in) -> {
          if (in instanceof SingleValueSet) {
            final SingleValueSet singleValueSet = (SingleValueSet) in;
            agg.setTimestamp(minTimestamp(agg.getTimestamp(), singleValueSet.getTimestamp()));
            final double value = singleValueSet.getValue();
            agg.setMin(Double.min(agg.getMin(), value));
            agg.setMax(Double.max(agg.getMax(), value));
            agg.setSum(agg.getSum()+value);
            agg.setCount(agg.getCount() + 1);
          } else {
            combineGauges(agg, ((AggregatedValueSet) in));
          }
        },
        ValueSetCollectors::combineGauges,
        agg -> {
          agg.setTimestamp(agg.getTimestamp().with(new TemporalNormalizer(windowSize)));
          agg.setAverage(agg.getCount() > 0 ? agg.getSum() / agg.getCount() : Double.NaN);
          agg.setGranularity(windowSize);
          return agg;
        }
    );
  }

  public static Collector<ValueSet, AggregatedValueSet, AggregatedValueSet> counterCollector(Duration windowSize) {
    return Collector.of(
        AggregatedValueSet::new,
        (agg, in) -> {
          if (in instanceof SingleValueSet) {
            final SingleValueSet singleValueSet = (SingleValueSet) in;
            agg.setTimestamp(minTimestamp(agg.getTimestamp(), singleValueSet.getTimestamp()));
            final double value = singleValueSet.getValue();
            agg.setSum(agg.getSum()+value);
          } else {
            combineCounters(agg, ((AggregatedValueSet) in));
          }
        },
        ValueSetCollectors::combineCounters,
        agg -> {
          agg.setTimestamp(agg.getTimestamp().with(new TemporalNormalizer(windowSize)));
          agg.setGranularity(windowSize);
          return agg;
        }
    );
  }

  private static AggregatedValueSet combineGauges(AggregatedValueSet agg, AggregatedValueSet in) {
    agg.setTimestamp(minTimestamp(agg.getTimestamp(), in.getTimestamp()));
    agg.setMin(Double.min(agg.getMin(), in.getMin()));
    agg.setMax(Double.max(agg.getMax(), in.getMax()));
    agg.setSum(Double.sum(agg.getSum(), in.getSum()));
    agg.setCount(Double.sum(agg.getCount(), in.getCount()));
    return agg;
  }

  private static AggregatedValueSet combineCounters(AggregatedValueSet agg, AggregatedValueSet in) {
    agg.setTimestamp(minTimestamp(agg.getTimestamp(), in.getTimestamp()));
    agg.setSum(Double.sum(agg.getSum(), in.getSum()));
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
