package com.rackspace.ceres.app.model;

import java.time.Duration;
import java.util.Map;

import com.rackspace.ceres.app.downsample.Aggregator;

import lombok.Data;

@Data
public class TsdbQuery {
  String metricName;
  Map<String,String> tags;
  Aggregator aggregator;
  Duration granularity;
}
