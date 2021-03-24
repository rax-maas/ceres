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
    if(type == SuggestType.tagk)
      return suggestApiService.suggestTagKeys(tenant, q, max);
    else if(type == SuggestType.tagv)
      return suggestApiService.suggestTagValues(tenant, q, max);
    else
      return suggestApiService.suggestMetricNames(tenant, q, max);
  }
}
