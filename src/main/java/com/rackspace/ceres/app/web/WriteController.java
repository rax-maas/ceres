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

package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.PutResponse;
import com.rackspace.ceres.app.services.DataWriteService;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

@RestController
@RequestMapping("/api")
@Slf4j
public class WriteController {

  private final DataWriteService dataWriteService;
  private final AppProperties appProperties;

  @Autowired
  public WriteController(DataWriteService dataWriteService, AppProperties appProperties) {
    this.dataWriteService = dataWriteService;
    this.appProperties = appProperties;
  }

  @PostMapping("/put")
  public Mono<ResponseEntity<?>> putMetrics(@RequestBody @Validated Flux<Metric> metrics,
                                            @RequestParam MultiValueMap<String, String> allParams,
                                            @RequestHeader(value = "X-Tenant", required = false) String tenantHeader
  ) {
    final Flux<Metric> results = dataWriteService.ingest(
        metrics
            .map(metric -> {
              String tenant;
              if (StringUtils.hasText(tenantHeader)) {
                tenant = tenantHeader;
              } else {
                tenant = metric.getTags().remove(appProperties.getTenantTag());

                if (!StringUtils.hasText(tenant)) {
                  tenant = appProperties.getDefaultTenant();
                }
              }

              return Tuples.of(tenant, metric);
            })
    );

    final boolean details = ParamUtils.paramPresentOrTrue(allParams, "details");
    final boolean summary = ParamUtils.paramPresentOrTrue(allParams, "summary");

    if (summary || details) {
      return results
          .count()
          .map(count ->
              new PutResponse()
                  .setSuccess(count)
                  // TODO set this with actual failed count
                  .setFailed(0)
                  // TODO set actual errored metrics
                  .setErrors(details ? List.of() : null)
          )
          .flatMap(putResponse ->
              Mono.just(ResponseEntity.ok(putResponse))
          );
    } else {
      return results.then(
          Mono.just(ResponseEntity.noContent().build())
      );
    }
  }

}
