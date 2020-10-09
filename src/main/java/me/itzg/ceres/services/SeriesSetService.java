package me.itzg.ceres.services;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import me.itzg.ceres.model.MetricNameAndTags;
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
