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

import com.rackspace.ceres.app.model.GetTagsResponse;
import com.rackspace.ceres.app.services.MetadataService;
import com.rackspace.ceres.app.validation.MetricNameAndGroupValidator;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Mono;
import springfox.documentation.annotations.ApiIgnore;

@RestController
@RequestMapping("/api/metadata")
@Profile("query")
@ApiImplicitParams(value = {
    @ApiImplicitParam(name = "X-Auth-Token", value = "Either of X-Auth Token or X-Username "
        + "and X-Password/X-Api-Key should be present", paramType = "header"),
    @ApiImplicitParam(name = "X-Username", value = "This header is required when X-Auth-Token "
        + "is not provide and it goes with X-Password or X-Api-Key headers", paramType = "header"),
    @ApiImplicitParam(name = "X-Password", value = "Required header if X-Username is given and X-Api-Key is not specified", paramType = "header"),
    @ApiImplicitParam(name = "X-Api-Key", value = "Required header if X-Username is given and X-Password is not specified", paramType = "header"),
})
public class MetadataController {

  private final MetadataService metadataService;
  private final MetricNameAndGroupValidator validator;

  private final Environment environment;

  @Autowired
  public MetadataController(MetadataService metadataService, MetricNameAndGroupValidator validator,
      Environment environment) {
    this.metadataService = metadataService;
    this.validator = validator;
    this.environment = environment;
  }

  @GetMapping("/tenants")
  @ApiIgnore
  public Mono<List<String>> getTenants() {
    if(isDevProfileActive()) {
      return metadataService.getTenants();
    }
    return Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND));
  }

  @GetMapping("/metricNames")
  @ApiOperation(value = "This api is used to get metric names for the given tenant")
  public Mono<List<String>> getMetricNames(
      @ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader) {
    return metadataService.getMetricNames(tenantHeader);
  }

  @GetMapping("/tagKeys")
  @ApiOperation(value = "This api is used to get metric tag keys for the given tenant and metric name")
  public Mono<List<String>> getTagKeys(
      @RequestParam String metricName,
      @ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader) {
    return metadataService
        .getTagKeys(tenantHeader, metricName);
  }

  @GetMapping("/tagValues")
  @ApiOperation(value = "This api is used to get tag values for the given tenant, metric name and tag key")
  public Mono<List<String>> getTagValues(
      @RequestParam String metricName,
      @RequestParam String tagKey,
      @ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader) {
    return metadataService
        .getTagValues(tenantHeader, metricName, tagKey);
  }

  @GetMapping("/tags")
  public Mono<GetTagsResponse> getTags(
      @RequestParam(required = false) String metricName,
      @RequestParam(required = false) String metricGroup,
      @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader) {
    validator.validateMetricNameAndMetricGroup(metricName, metricGroup);

    return metadataService
        .getTags(tenantHeader, metricName, metricGroup);
  }

  public boolean isDevProfileActive() {
    return environment.acceptsProfiles(Profiles.of("dev"));
  }
}
