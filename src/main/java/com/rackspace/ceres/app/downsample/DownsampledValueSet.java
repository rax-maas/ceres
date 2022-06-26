package com.rackspace.ceres.app.downsample;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.time.Duration;

@Data
@EqualsAndHashCode(callSuper = true)
public class DownsampledValueSet extends ValueSet {
  double value;
  int count;
}
