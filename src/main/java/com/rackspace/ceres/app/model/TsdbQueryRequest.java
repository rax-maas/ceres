package com.rackspace.ceres.app.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class TsdbQueryRequest {
  String metric;
  String downsample;
  List<Map<String, String>> filters;
}
