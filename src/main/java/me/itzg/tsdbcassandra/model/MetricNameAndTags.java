package me.itzg.tsdbcassandra.model;

import java.util.Map;
import lombok.Data;

@Data
public class MetricNameAndTags {
  String metricName;
  Map<String,String> tags;
}