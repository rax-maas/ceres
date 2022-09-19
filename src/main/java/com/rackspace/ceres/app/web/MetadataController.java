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

import com.rackspace.ceres.app.model.Criteria;
import com.rackspace.ceres.app.model.MetricDTO;
import com.rackspace.ceres.app.services.ElasticSearchService;
import com.rackspace.ceres.app.services.MetadataService;
import io.swagger.annotations.ApiOperation;
import java.io.IOException;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
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
public class MetadataController {

  private final MetadataService metadataService;
  private final ElasticSearchService elasticSearchService;

  private final Environment environment;

  @Autowired
  public MetadataController(MetadataService metadataService, ElasticSearchService elasticSearchService,
      Environment environment) {
    this.metadataService = metadataService;
    this.elasticSearchService = elasticSearchService;
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

  @PostMapping(path = "/search")
  @ApiOperation(value = "This api is used to get all the metric info based upon given filter, include and exclude fields in Criteria object.")
  public List<MetricDTO> search(@ApiIgnore @RequestHeader(value = "#{appProperties.tenantHeader}") String tenantHeader,
      @RequestParam(required = false) String maskTenantId,
      @RequestBody Criteria criteria)
      throws IOException {
    //TODO Need to remove this code as this is part of testing and demo purpose only.
    if(maskTenantId!=null) {
      tenantHeader = maskTenantId;
    }
    return elasticSearchService.search(tenantHeader, criteria);
  }

  public boolean isDevProfileActive() {
    return environment.acceptsProfiles(Profiles.of("dev"));
  }
}
