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

import com.rackspace.ceres.app.services.MetadataService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/metadata")
public class MetadataController {

  private final MetadataService metadataService;

  @Autowired
  public MetadataController(MetadataService metadataService) {
    this.metadataService = metadataService;
  }

  @GetMapping("/tenants")
  public Mono<List<String>> getTenants() {
    return metadataService.getTenants();
  }

  @GetMapping("/metricNames")
  public Mono<List<String>> getMetricNames(@RequestParam String tenant) {
    return metadataService.getMetricNames(tenant);
  }

  @GetMapping("/tagKeys")
  public Mono<List<String>> getTagKeys(@RequestParam String tenant,
                                       @RequestParam String metricName) {
    return metadataService.getTagKeys(tenant, metricName);
  }

  @GetMapping("/tagValues")
  public Mono<List<String>> getTagValues(@RequestParam String tenant,
                                         @RequestParam String metricName,
                                         @RequestParam String tagKey) {
    return metadataService.getTagValues(tenant, metricName, tagKey);
  }
}
