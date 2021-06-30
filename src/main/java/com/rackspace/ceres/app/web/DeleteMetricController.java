package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.services.MetricDeletionService;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import java.time.Instant;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks.Empty;
import springfox.documentation.annotations.ApiIgnore;

@RestController
@RequestMapping("/api/metric")
@Profile("admin")
@ApiIgnore
public class DeleteMetricController {

  private final MetricDeletionService metricDeletionService;

  @Autowired
  public DeleteMetricController(MetricDeletionService metricDeletionService) {
    this.metricDeletionService = metricDeletionService;
  }

  @DeleteMapping
  public Mono<Empty> deleteMetrics(@RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader,
      @RequestParam(required = false) String metricName,
      @RequestParam(required = false) String metricGroup,
      @RequestParam(required = false) List<String> tag,
      @RequestParam String start,
      @RequestParam(required = false) String end) {
    Instant startTime = DateTimeUtils.parseInstant(start);
    Instant endTime = DateTimeUtils.parseInstant(end);
    return metricDeletionService.deleteMetrics(tenantHeader, metricName, tag, startTime, endTime, metricGroup);
  }
}
