/*
 * Copyright 2021 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.rackspace.ceres.app.validation;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

@Component
public class MetricNameAndGroupValidator {

  public void validateMetricNameAndGroup(String metricName, String metricGroup) {
    if(!StringUtils.hasText(metricGroup) && !StringUtils.hasText(metricName)) {
      throw new IllegalArgumentException("metricGroup and metricName both cannot be empty");
    }

    if(StringUtils.hasText(metricGroup) && StringUtils.hasText(metricName)) {
      throw new IllegalArgumentException("metricGroup and metricName both cannot be non-empty");
    }
  }
}
