package me.itzg.tsdbcassandra.web;

import java.util.List;
import me.itzg.tsdbcassandra.services.MetadataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/metadata")
public class MetadataController {

  private final MetadataService metadataService;

  @Autowired
  public MetadataController(MetadataService metadataService) {
    this.metadataService = metadataService;
  }

  @GetMapping("/metricNames")
  public Mono<List<String>> getMetricNames(@RequestParam String tenant) {
    return metadataService.getMetricNames(tenant);
  }

  @GetMapping("/tagKeys")
  public Mono<List<String>> getTagKeys(@RequestParam String tenant,
                                       @RequestParam String metricName) {
    return metadataService.getTagKeys(tenant, metricName);
  }

  @GetMapping("/tagValues")
  public Mono<List<String>> getTagValues(@RequestParam String tenant,
                                         @RequestParam String metricName,
                                         @RequestParam String tagKey) {
    return metadataService.getTagValues(tenant, metricName, tagKey);
  }
}
