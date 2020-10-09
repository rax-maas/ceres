/*
 * Copyright 2020 Rackspace US, Inc.
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
 */

package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.model.MetricNameAndTags;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.springframework.stereotype.Service;

@Service
public class SeriesSetService {

  public String buildSeriesSet(String metricName, Map<String, String> tags) {
    return metricName + "," +
        tags.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(tagsEntry -> tagsEntry.getKey() + "=" + tagsEntry.getValue())
            .collect(Collectors.joining(","));
  }

  public String metricNameFromSeriesSet(String seriesSet) {
    final String[] parts = seriesSet.split(",");
    return parts[0];
  }

  public MetricNameAndTags expandSeriesSet(String seriesSet) {
    final String[] pairs = seriesSet.split(",");
    final String metricName = pairs[0];
    final Map<String, String> tags = new HashMap<>(pairs.length - 1);
    for (int i = 1; i < pairs.length; i++) {
      final String[] kv = pairs[i].split("=", 2);
      tags.put(kv[0], kv[1]);
    }

    return new MetricNameAndTags()
        .setMetricName(metricName)
        .setTags(tags);
  }

  public boolean isCounter(String seriesSet) {
    //TODO resolve by matching metric name suffix to known counter suffixes
    return false;
  }
}
