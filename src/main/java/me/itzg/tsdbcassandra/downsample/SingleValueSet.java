package me.itzg.tsdbcassandra.downsample;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class SingleValueSet extends ValueSet {
  double value;
}
