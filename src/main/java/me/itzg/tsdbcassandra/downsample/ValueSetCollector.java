package me.itzg.tsdbcassandra.downsample;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

public class ValueSetCollector implements Collector<ValueSet, AggregatedValueSet, AggregatedValueSet> {

  @Override
  public Supplier<AggregatedValueSet> supplier() {
    return null;
  }

  @Override
  public BiConsumer<AggregatedValueSet, ValueSet> accumulator() {
    return (acc, in) -> {
      if (in instanceof Gauge) {
        final double value = ((Gauge) in).getValue();
        acc.setMin(Double.min(acc.getMin(), value));
        acc.setMax(Double.max(acc.getMax(), value));
        acc.setSum(acc.getSum() + value);
        acc.setCount(acc.getCount() + 1);
      } else if (in instanceof Counter) {
        final double value = ((Counter) in).getValue();
        acc.setSum(acc.getSum() + value);
      } else if (in instanceof AggregatedValueSet) {
        final AggregatedValueSet agg = (AggregatedValueSet) in;
        combine(acc, agg);
      }
    };
  }

  private static AggregatedValueSet combine(AggregatedValueSet acc, AggregatedValueSet in) {
    if (in.getMin() != null) {
      acc.setMin(Double.min(acc.getMin(), in.getMin()));
    }
    if (in.getMax() != null) {
      acc.setMax(Double.max(acc.getMax(), in.getMax()));
    }
    if (in.getSum() != null) {
      acc.setSum(acc.getSum() + in.getSum());
    }
    if (in.getCount() != null) {
      acc.setCount(acc.getCount() + in.getCount());
    }
    return acc;
  }

  @Override
  public BinaryOperator<AggregatedValueSet> combiner() {
    return ValueSetCollector::combine;
  }

  @Override
  public Function<AggregatedValueSet, AggregatedValueSet> finisher() {
    return Function.identity();
  }

  @Override
  public Set<Characteristics> characteristics() {
    return Set.of(
        Characteristics.IDENTITY_FINISH
    );
  }
}
