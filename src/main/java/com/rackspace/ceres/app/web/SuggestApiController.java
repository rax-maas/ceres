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

import com.rackspace.ceres.app.model.SuggestType;
import com.rackspace.ceres.app.services.SuggestApiService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/suggest")
public class SuggestApiController {

  private final SuggestApiService suggestApiService;

  @Autowired
  public SuggestApiController(SuggestApiService suggestApiService) {
    this.suggestApiService = suggestApiService;
  }

  /**
   * This endpoint provides a means of implementing an auto-complete.
   *
   * @param tenant tenant of the customer
   * @param type   This is the type of data we want to suggest. It can be anything from {@link
   *               SuggestType}
   * @param q      This is the text for which we have to suggest matching entries.
   * @param max    Limit on number of results to return
   * @return list of string containing the auto complete suggestions.
   */
  @GetMapping
  public Mono<List<String>> getSuggestions(@RequestHeader("X-Tenant") String tenant,
      @RequestParam SuggestType type, @RequestParam(required = false) String q,
      @RequestParam(required = false, defaultValue = "25") int max) {

    return switch (type) {
      case TAGK ->  suggestApiService.suggestTagKeys(tenant, q, max);
      case TAGV -> suggestApiService.suggestTagValues(tenant, q, max);
      case METRICS -> suggestApiService.suggestMetricNames(tenant, q, max);
    };
  }
}