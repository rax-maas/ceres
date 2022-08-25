/*
 * Copyright 2022 Rackspace US, Inc.
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
 *
 */

package com.rackspace.ceres.app.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.Criteria;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Slf4j
@Service
public class ElasticSearchService {

  private AppProperties appProperties;

  private RestHighLevelClient restHighLevelClient;

  private ObjectMapper objectMapper;

  @Autowired
  public ElasticSearchService(RestHighLevelClient restHighLevelClient, AppProperties appProperties,
      ObjectMapper objectMapper) {
    this.restHighLevelClient = restHighLevelClient;
    this.appProperties = appProperties;
    this.objectMapper = objectMapper;
  }

  /**
   * index metric entity to es
   * using tenantId as routing param.
   *
   * @param tenant the tenant
   * @param metric the metric
   * @return the mono
   */
  public Mono<Void> saveMetricToES(String tenant, Metric metric) {
    com.rackspace.ceres.app.entities.Metric metricEntity = new com.rackspace.ceres.app.entities.Metric();
    metricEntity.setMetricName(metric.getMetric());
    metricEntity.setTenant(tenant);
    metricEntity.setTags(metric.getTags());
    log.trace("saving metric {} to ES ", metric);
    IndexRequest indexRequest = new IndexRequest();
    try {
      indexRequest.index(appProperties.getIndexName());
      indexRequest.source(objectMapper.writeValueAsString(metricEntity), XContentType.JSON);
      indexRequest.routing(tenant);
      restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return Mono.empty();
  }

  /**
   * performs a search operation on ES using given filters in Criteria object
   * returns list of MetricDTO
   *
   * @param tenantId the tenant id
   * @param criteria the criteria
   * @return the list
   * @throws IOException the io exception
   */
  public List<MetricDTO> search(String tenantId, Criteria criteria)
      throws IOException {
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.must(QueryBuilders.termQuery("tenant", tenantId));

    if(criteria.getFilter() != null) {
      criteria.getFilter().stream().filter(Objects::nonNull).forEach(filter ->  {
            String filterKey = filter.getFilterKey();
            String filterValue = filter.getFilterValue();
            if(filterKey.equals("metricName")) {
              query.must(QueryBuilders.wildcardQuery(filterKey, filterValue));
            } else {
              query.must(QueryBuilders.wildcardQuery(filterKey+".keyword", filterValue));
            }
          }
      );
    }
    searchSourceBuilder.query(query);

    searchSourceBuilder.fetchSource(
        criteria.getIncludeFields() == null ? null
            : Arrays.stream(criteria.getIncludeFields().toArray()).toArray(String[]::new),
        criteria.getExcludeFields() == null ? null
            : Arrays.stream(criteria.getExcludeFields().toArray()).toArray(String[]::new));

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(appProperties.getIndexName());
    searchRequest.source(searchSourceBuilder);
    searchRequest.routing(tenantId);
    SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
    List<MetricDTO> metrics = new ArrayList<>();
    searchResponse.getHits().forEach(e -> {
      MetricDTO metric = null;
      try {
        metric = objectMapper.readValue(e.getSourceAsString(), MetricDTO.class);
      } catch (Exception e1) {
        log.error("exception thrown while converting MetricDTO object",e1);
      }
      metrics.add(metric);
    });
    return metrics;
  }
}
