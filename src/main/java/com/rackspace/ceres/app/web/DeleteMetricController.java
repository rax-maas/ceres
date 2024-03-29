package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.services.MetricDeletionService;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import java.time.Instant;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
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
      @RequestParam(required = false) String start,
      @RequestParam(required = false) String end) {
    Instant startTime = null;
    if(start != null) {
      startTime = DateTimeUtils.parseInstant(start);
    }
    Instant endTime = DateTimeUtils.parseInstant(end);
    return metricDeletionService.deleteMetricsByTenantId(tenantHeader, startTime, endTime);
  }

  @DeleteMapping("/metricName/{metricName}")
  public Mono<Empty> deleteMetricByMetricName(@RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader,
      @PathVariable String metricName,
      @RequestParam(required = false) List<String> tag,
      @RequestParam(required = false) String start,
      @RequestParam(required = false) String end) {
    Instant startTime = null;
    if(start != null) {
      startTime = DateTimeUtils.parseInstant(start);
    }
    Instant endTime = DateTimeUtils.parseInstant(end);
    if(CollectionUtils.isEmpty(tag)) {
      return metricDeletionService.deleteMetricsByMetricName(tenantHeader, metricName, startTime, endTime);
    } else {
      return metricDeletionService.deleteMetricsByMetricNameAndTag(tenantHeader, metricName, tag, startTime, endTime);
    }
  }

  @DeleteMapping("/metricGroup/{metricGroup}")
  public Mono<Empty> deleteMetricByMetricGroup(@RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader,
      @PathVariable String metricGroup,
      @RequestParam(required = false) String start,
      @RequestParam(required = false) String end) {
    Instant startTime = null;
    if(start != null) {
      startTime = DateTimeUtils.parseInstant(start);
    }
    Instant endTime = DateTimeUtils.parseInstant(end);
    return metricDeletionService.deleteMetricsByMetricGroup(tenantHeader, metricGroup, startTime, endTime);
  }
}
