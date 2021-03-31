/*
 * Copyright 2021 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.ceres.app.web;

import static com.rackspace.ceres.app.web.TagListConverter.convertPairsListToMap;

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.model.QueryResult;
import com.rackspace.ceres.app.services.QueryService;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Native Ceres query API endpoints.
 */
@RestController
@RequestMapping("/api/query")
@Profile("query")
public class QueryController {

  private final QueryService queryService;
  private final DownsampleProperties downsampleProperties;
  private final Counter aggregatorQueryCounter;
  private final Counter noAggregatorQueryCounter;

  @Autowired
  public QueryController(QueryService queryService, DownsampleProperties downsampleProperties,
      MeterRegistry meterRegistry) {
    this.queryService = queryService;
    this.downsampleProperties = downsampleProperties;
    noAggregatorQueryCounter = meterRegistry.counter("Ceres.QueryApi", "Query", "Without_Aggregator");
    aggregatorQueryCounter = meterRegistry.counter("Ceres.QueryApi", "Query", "With_Aggregator");
  }

  @GetMapping
  public Flux<QueryResult> query(@RequestParam(name = "tenant") String tenantParam,
      @RequestParam String metricName,
      @RequestParam(defaultValue = "raw") Aggregator aggregator,
      @RequestParam(required = false) Duration granularity,
      @RequestParam List<String> tag,
      @RequestParam String start,
      @RequestParam(required = false) String end) {
    Instant startTime = DateTimeUtils.parseInstant(start);
    Instant endTime = DateTimeUtils.parseInstant(end);

    if (aggregator == null || Objects.equals(aggregator, Aggregator.raw)) {
      noAggregatorQueryCounter.increment();
      return queryService.queryRaw(tenantParam, metricName,
          convertPairsListToMap(tag),
          startTime, endTime
      );
    } else {
      if (granularity == null) {
        granularity = DateTimeUtils
            .getGranularity(startTime, endTime, downsampleProperties.getGranularities());
      }
      aggregatorQueryCounter.increment();
      return queryService.queryDownsampled(tenantParam, metricName,
          aggregator,
          granularity,
          convertPairsListToMap(tag),
          startTime, endTime
      );
    }
  }
}
