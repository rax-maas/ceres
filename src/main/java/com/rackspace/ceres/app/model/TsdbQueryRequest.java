package com.rackspace.ceres.app.model;

import java.util.List;

import lombok.Data;

@Data
public class TsdbQueryRequest {
  String metric;
  String downsample;
  List<TsdbFilter> filters;
}
