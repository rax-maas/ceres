package com.rackspace.ceres.app.model;

import java.util.List;
import java.util.Map;

import lombok.Data;

@Data
public class MetricNameAndMultiTags {
    String metricName;
    List<Map<String, String>> tags;
}
