package com.rackspace.ceres.app.model;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import com.rackspace.ceres.app.downsample.Aggregator;

import lombok.Data;

@Data
public class TsdbQuery {
  String tenant;
  Instant start;
  Instant end;
  String seriesSet;
  String metricName;
  Map<String,String> tags;
  Aggregator aggregator;
  Duration granularity;
}
