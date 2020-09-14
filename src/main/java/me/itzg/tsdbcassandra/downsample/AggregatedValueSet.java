package me.itzg.tsdbcassandra.downsample;

import java.time.Duration;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class AggregatedValueSet extends ValueSet {
  double min = Double.MAX_VALUE;
  double max = Double.MIN_VALUE;
  double sum;
  double count;
  double average = Double.NaN;
  Duration granularity;
}
