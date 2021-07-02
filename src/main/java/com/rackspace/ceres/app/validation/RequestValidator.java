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

import java.util.List;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

public class RequestValidator {

  public static void validateMetricNameAndGroup(String metricName, String metricGroup) {
    if(!StringUtils.hasText(metricGroup) && !StringUtils.hasText(metricName)) {
      throw new IllegalArgumentException("metricGroup and metricName both cannot be empty");
    }

    if(StringUtils.hasText(metricGroup) && StringUtils.hasText(metricName)) {
      throw new IllegalArgumentException("metricGroup and metricName both cannot be non-empty");
    }
  }

  public static void validateMetricGroupAndTags(String metricGroup, List<String> tags) {
    if(StringUtils.hasText(metricGroup)) {
      if (!ObjectUtils.isEmpty(tags)) {
        throw new IllegalArgumentException("Tags are not required when passing metric group");
      }
    }
  }
}
