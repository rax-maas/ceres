package me.itzg.tsdbcassandra.web;

import me.itzg.tsdbcassandra.config.AppProperties;
import me.itzg.tsdbcassandra.model.Metric;
import me.itzg.tsdbcassandra.services.IngestService;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuples;

@RestController
@RequestMapping("/api")
public class WriteController {

  private final IngestService ingestService;
  private final AppProperties appProperties;

  @Autowired
  public WriteController(IngestService ingestService, AppProperties appProperties) {
    this.ingestService = ingestService;
    this.appProperties = appProperties;
  }

  @PostMapping("/put")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  public Publisher<?> writeSingle(@RequestBody @Validated Flux<Metric> metrics,
                                  @RequestHeader(value = "X-Tenant", required = false) String tenantHeader
  ) {
    return ingestService.ingest(
        metrics.map(metric -> {
          if (tenantHeader != null) {
            return Tuples.of(tenantHeader, metric);
          }

          final String tenant = metric.getTags().remove(appProperties.getTenantTag());

          return Tuples.of(tenant != null ? tenant : appProperties.getDefaultTenant(), metric);
        })

    );
  }
}
