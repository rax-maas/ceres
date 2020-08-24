package me.itzg.tsdbcassandra.web;

import static me.itzg.tsdbcassandra.web.TagListConverter.convertPairsListToMap;

import java.time.Instant;
import java.util.List;
import me.itzg.tsdbcassandra.model.QueryResult;
import me.itzg.tsdbcassandra.services.QueryService;
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
                                 @RequestParam List<String> tag,
                                 @RequestParam Instant start,
                                 @RequestParam Instant end) {
    return queryService.query(tenant, metricName,
        convertPairsListToMap(tag),
        start, end
        );
  }
}
