package com.rackspace.ceres.app.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class TsdbQueryResult {
  String metric;
  Map<String,String> tags;
  List<String> aggregatedTags;
  Map<String, Double> dps;
}
