package me.itzg.ceres.model;

import java.time.Instant;
import java.util.Map;
import lombok.Data;

@Data
public class QueryResult {
  String tenant;
  String metricName;
  Map<String,String> tags;
  Map<Instant, Double> values;
}
