package com.rackspace.ceres.app.validator;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

@Component
public class QueryRequestValidator {

  public void validateMetricNameAndMetricGroup(String metricName, String metricGroup) {
    if(StringUtils.isEmpty(metricGroup) && StringUtils.isEmpty(metricName)) {
      throw new IllegalArgumentException("metricGroup and metricName both cannot be empty");
    }

    if(StringUtils.isNotEmpty(metricGroup) && StringUtils.isNotEmpty(metricName)) {
      throw new IllegalArgumentException("metricGroup and metricName both cannot be non-empty");
    }
  }
}
