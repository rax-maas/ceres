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
import com.rackspace.ceres.app.services.MetadataService;
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

@RestController
@RequestMapping("/api/metadata")
@Profile("query")
public class MetadataController {

  private final MetadataService metadataService;

  private final Environment environment;

  private final AppProperties appProperties;

  @Autowired
  public MetadataController(MetadataService metadataService, Environment environment,
      AppProperties appProperties) {
    this.metadataService = metadataService;
    this.environment = environment;
    this.appProperties = appProperties;
  }

  @GetMapping("/tenants")
  public Mono<List<String>> getTenants() {
    if(isDevProfileActive()) {
      return metadataService.getTenants();
    }
    return Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND));
  }

  @GetMapping("/metricNames")
  public Mono<List<String>> getMetricNames(
      @RequestParam(name = "tenant", required = false) String tenantParam,
      @RequestHeader(value = "#{appProperties.tenantHeader}", required = false) String tenantHeader) {
    return metadataService.getMetricNames(ParamUtils.resolveTenant(tenantParam, tenantHeader));
  }

  @GetMapping("/tagKeys")
  public Mono<List<String>> getTagKeys(
      @RequestParam(name = "tenant", required = false) String tenantParam,
      @RequestParam String metricName,
      @RequestHeader(value = "#{appProperties.tenantHeader}", required = false) String tenantHeader) {
    return metadataService
        .getTagKeys(ParamUtils.resolveTenant(tenantParam, tenantHeader), metricName);
  }

  @GetMapping("/tagValues")
  public Mono<List<String>> getTagValues(
      @RequestParam(name = "tenant", required = false) String tenantParam,
      @RequestParam String metricName,
      @RequestParam String tagKey,
      @RequestHeader(value = "#{appProperties.tenantHeader}", required = false) String tenantHeader) {
    return metadataService
        .getTagValues(ParamUtils.resolveTenant(tenantParam, tenantHeader), metricName, tagKey);
  }

  public boolean isDevProfileActive() {
    return environment.acceptsProfiles(Profiles.of("dev"));
  }
}
