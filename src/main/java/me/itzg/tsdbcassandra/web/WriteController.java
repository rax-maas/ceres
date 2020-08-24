package me.itzg.tsdbcassandra.web;

import me.itzg.tsdbcassandra.model.Metric;
import me.itzg.tsdbcassandra.services.IngestService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/write")
public class WriteController {

  private final IngestService ingestService;

  @Autowired
  public WriteController(IngestService ingestService) {
    this.ingestService = ingestService;
  }

  @PostMapping("/single")
  public Mono<?> writeSingle(@RequestBody @Validated Metric metric) {
    return ingestService.ingest(metric);
  }
}
