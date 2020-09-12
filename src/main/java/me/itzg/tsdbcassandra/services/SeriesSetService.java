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
}
