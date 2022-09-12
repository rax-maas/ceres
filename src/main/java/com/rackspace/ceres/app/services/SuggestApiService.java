package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.model.SuggestType;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Profile("query")
public class SuggestApiService {

  private final MetadataService metadataService;

  @Autowired
  public SuggestApiService(MetadataService metadataService) {
    this.metadataService = metadataService;
  }

  /**
   * Method used to return the list of tag keys associated with particular tenant with limit set to
   * max
   *
   * @param tenant
   * @param tagK
   * @param max
   * @return
   */
  public Mono<List<String>> suggestTagKeys(String tenant, String tagK, int max) {
    return metadataService.getTagKeysOrValuesForTenant(tenant, SuggestType.TAGK).flatMapMany(Flux::fromIterable)
        .filter(tagValue -> !StringUtils.hasText(tagK) || tagValue.startsWith(tagK))
        .distinct()
        .take(max)
        .collectList();
  }

  /**
   * Method used to return the list of metric names that starts with metric for a tenant and limits
   * the result to max.
   *
   * @param tenant
   * @param metric
   * @param max
   * @return
   */
  public Mono<List<String>> suggestMetricNames(String tenant, String metric, int max) {
    return metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable)
        .filter(metricName -> !StringUtils.hasText(metric) || metricName.startsWith(metric))
        .take(max)
        .collectList();
  }

  /**
   * Method used to return the list of tag values that starts with tagV for a tenant and limits
   * the result to max.
   *
   * @param tenant
   * @param tagV
   * @param max
   * @return
   */
  public Mono<List<String>> suggestTagValues(String tenant, String tagV, int max) {
    return metadataService.getTagKeysOrValuesForTenant(tenant, SuggestType.TAGV).flatMapMany(Flux::fromIterable)
        .filter(tagValue -> !StringUtils.hasText(tagV) || tagValue.startsWith(tagV))
        .distinct()
        .take(max)
        .collectList();
  }
}
