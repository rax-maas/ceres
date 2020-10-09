package me.itzg.ceres.downsample;

import java.time.Duration;
import java.time.Instant;
import lombok.Data;

@Data
public class DataDownsampled {
  String tenant;

  String seriesSet;

  Aggregator aggregator;

  Duration granularity;

  Instant ts;

  double value;
}
