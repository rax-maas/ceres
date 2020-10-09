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

package me.itzg.ceres.web;

import java.util.List;
import me.itzg.ceres.config.AppProperties;
import me.itzg.ceres.model.Metric;
import me.itzg.ceres.model.PutResponse;
import me.itzg.ceres.services.DataWriteService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
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
              if (tenantHeader != null) {
                return Tuples.of(tenantHeader, metric);
              }

              final String tenant = metric.getTags().remove(appProperties.getTenantTag());

              return Tuples.of(tenant != null ? tenant : appProperties.getDefaultTenant(), metric);
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
