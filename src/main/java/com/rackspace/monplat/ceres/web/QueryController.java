/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.monplat.ceres.web;

import static com.rackspace.monplat.ceres.web.TagListConverter.convertPairsListToMap;

import com.rackspace.monplat.ceres.downsample.Aggregator;
import com.rackspace.monplat.ceres.model.QueryResult;
import com.rackspace.monplat.ceres.services.QueryService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
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
