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
import com.rackspace.ceres.app.model.TsdbQueryRequestData;
import com.rackspace.ceres.app.model.TsdbQueryResult;
import com.rackspace.ceres.app.services.QueryService;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import com.rackspace.ceres.app.validation.MetricNameAndGroupValidator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import springfox.documentation.annotations.ApiIgnore;

/**
 * Native Ceres query API endpoints.
 */
@RestController
@RequestMapping("/api/query")
@Profile("query")
@ApiImplicitParams(value = {
    @ApiImplicitParam(name = "X-Auth-Token", value = "Either of X-Auth Token or X-Username "
        + "and X-Password/X-Api-Key should be present", paramType = "header"),
    @ApiImplicitParam(name = "X-Username", value = "This header is required when X-Auth-Token "
        + "is not provide and it goes with X-Password or X-Api-Key headers", paramType = "header"),
    @ApiImplicitParam(name = "X-Password", value = "Required header if X-Username is given and X-Api-Key is not specified", paramType = "header"),
    @ApiImplicitParam(name = "X-Api-Key", value = "Required header if X-Username is given and X-Password is not specified", paramType = "header"),
})
public class QueryController {

  private final QueryService queryService;
  private final MetricNameAndGroupValidator validator;
  private final DownsampleProperties downsampleProperties;
  private final Counter rawQueryCounter;
  private final Counter downSampleQueryCounter;

  @Autowired
  public QueryController(QueryService queryService, MetricNameAndGroupValidator validator,
      DownsampleProperties downsampleProperties, MeterRegistry meterRegistry) {
    this.queryService = queryService;
    this.validator = validator;
    this.downsampleProperties = downsampleProperties;
    rawQueryCounter = meterRegistry.counter("ceres.query", "type", "raw");
    downSampleQueryCounter = meterRegistry.counter("ceres.query", "type", "downsample");
  }

  @PostMapping
  @ApiOperation(value = "This api is used to query metrics data and is modelled as opentsdb query api")
  public Flux<TsdbQueryResult> queryTsdb(@RequestBody TsdbQueryRequestData timeQueryData,
                                         @ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader) {
    Instant startTime = DateTimeUtils.parseInstant(timeQueryData.getStart());
    Instant endTime = DateTimeUtils.parseInstant(timeQueryData.getEnd());
    return queryService
            .queryTsdb(tenantHeader, timeQueryData.getQueries(), startTime,
                    endTime, downsampleProperties.getGranularities());
  }

  @GetMapping
  @ApiOperation(value = "This api is used to get metric data based on request parameters")
  public Flux<QueryResult> query(
      @ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader,
      @RequestParam(required = false) String metricName,
      @RequestParam(required = false) String metricGroup,
      @RequestParam(defaultValue = "raw") Aggregator aggregator,
      @RequestParam(required = false) Duration granularity,
      @RequestParam List<String> tag,
      @RequestParam String start,
      @RequestParam(required = false) String end) {
    validator.validateMetricNameAndMetricGroup(metricName, metricGroup);

    Instant startTime = DateTimeUtils.parseInstant(start);
    Instant endTime = DateTimeUtils.parseInstant(end);

    if (aggregator == null || Objects.equals(aggregator, Aggregator.raw)) {
        rawQueryCounter.increment();
        return queryService.queryRaw(tenantHeader, metricName, metricGroup,
            convertPairsListToMap(tag),
            startTime, endTime
        );
    } else {
      if (granularity == null) {
        granularity = DateTimeUtils
            .getGranularity(startTime, endTime, downsampleProperties.getGranularities());
      }
      downSampleQueryCounter.increment();
      return queryService.queryDownsampled(tenantHeader, metricName, metricGroup,
          aggregator,
          granularity,
          convertPairsListToMap(tag),
          startTime, endTime
      );
    }
  }
}
