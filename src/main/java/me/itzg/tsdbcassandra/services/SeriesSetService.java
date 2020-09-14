package me.itzg.tsdbcassandra.services;

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

  public boolean isCounter(String seriesSet) {
    //TODO resolve by matching metric name suffix to known counter suffixes
    return false;
  }
}
