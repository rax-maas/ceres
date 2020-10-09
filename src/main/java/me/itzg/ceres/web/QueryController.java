package me.itzg.ceres.web;

import static me.itzg.ceres.web.TagListConverter.convertPairsListToMap;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import me.itzg.ceres.downsample.Aggregator;
import me.itzg.ceres.model.QueryResult;
import me.itzg.ceres.services.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/query")
public class QueryController {

  private final QueryService queryService;

  @Autowired
  public QueryController(QueryService queryService) {
    this.queryService = queryService;
  }

  @GetMapping
  public Flux<QueryResult> query(@RequestParam String tenant,
                                 @RequestParam String metricName,
                                 @RequestParam(defaultValue = "raw") Aggregator aggregator,
                                 @RequestParam(required = false) Duration granularity,
                                 @RequestParam List<String> tag,
                                 @RequestParam Instant start,
                                 @RequestParam Instant end) {

    if (aggregator == null || Objects.equals(aggregator, Aggregator.raw)) {
      return queryService.queryRaw(tenant, metricName,
          convertPairsListToMap(tag),
          start, end
      );
    } else {
      if (granularity == null) {
        return Flux.error(
            new IllegalArgumentException("granularity is required when using aggregator")
        );
      } else {
        return queryService.queryDownsampled(tenant, metricName,
            aggregator,
            granularity,
            convertPairsListToMap(tag),
            start, end
        );
      }
    }

  }
}
