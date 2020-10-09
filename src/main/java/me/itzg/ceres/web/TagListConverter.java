package me.itzg.ceres.web;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TagListConverter {

  static Map<String, String> convertPairsListToMap(List<String> tag) {
    return tag.stream()
        .map(s -> s.split("=", 2))
        .filter(strings -> strings.length == 2)
        .collect(Collectors.toMap(parts -> parts[0], parts -> parts[1]));
  }
}
