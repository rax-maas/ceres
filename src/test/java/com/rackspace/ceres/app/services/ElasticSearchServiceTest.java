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

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.CassandraContainerSetup;
import com.rackspace.ceres.app.ESContainerSetup;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.Criteria;
import com.rackspace.ceres.app.model.Filter;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.MetricDTO;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@ActiveProfiles(profiles = {"test","query"})
@Testcontainers
public class ElasticSearchServiceTest {

  @Container
  public static CassandraContainer<?> cassandraContainer = new CassandraContainer<>(
      CassandraContainerSetup.DOCKER_IMAGE);
  @TestConfiguration
  @Import(CassandraContainerSetup.class)
  public static class TestConfig {
    @Bean
    CassandraContainer<?> cassandraContainer() {
      return cassandraContainer;
    }
  }

  @Container
  public static ESContainerSetup elasticsearchContainer;

  static {
    elasticsearchContainer = new ESContainerSetup();
    elasticsearchContainer.start();

    System.setProperty("spring.data.elasticsearch.client.reactive.endpoints",
        elasticsearchContainer.getContainerIpAddress()+":"+elasticsearchContainer.getFirstMappedPort());
    System.setProperty("ceres.elastic-search-host", elasticsearchContainer.getContainerIpAddress());
    System.setProperty("ceres.elastic-search-port", elasticsearchContainer.getFirstMappedPort() + "");
  }

  @Autowired
  ElasticSearchService elasticSearchService;

  @Autowired
  ObjectMapper objectMapper;

  @Autowired
  AppProperties appProperties;

  @Autowired
  RestHighLevelClient restHighLevelClient;

  @AfterEach
  void tearDown() throws IOException {
    DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest("metrics");
    restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
  }

  @BeforeEach
  void testIsContainerRunning() throws IOException {
    Assert.assertTrue(elasticsearchContainer.isRunning());

    CreateIndexRequest request = new CreateIndexRequest("metrics");
    String mapping = "{\n    \"properties\": {\n        \"id\": {\n          \"type\": \"keyword\"\n        },\n        \"metricName\": {\n          \"type\": \"keyword\"\n        },\n        \"tenant\": {\n          \"type\": \"keyword\"\n        }\n    }\n  }";
    request.mapping(mapping, XContentType.JSON);
    restHighLevelClient.indices().create(request, RequestOptions.DEFAULT);
  }

  @BeforeAll
  static void setUp() {
    elasticsearchContainer.start();
  }

  @AfterAll
  static void destroy() {
    elasticsearchContainer.stop();
  }

  @Test
  public void testSearch() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);

    final String metricName1 = RandomStringUtils.randomAlphanumeric(10);
    final String metricName2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName1);
    Map<String, String> tags1 = Map.of("host", "h-1", "os", "linux", "deployment", "prod");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName2);
    Map<String, String> tags2 = Map.of("host", "h-2", "os", "windows", "deployment", "dev");
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();

    MetricDTO metricDTO1 = new MetricDTO(metricName1, tags1);
    MetricDTO metricDTO2 = new MetricDTO(metricName2, tags2);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    Thread.sleep(1000);
    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, new Criteria());
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchWithInvalidCriteria() throws IOException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = "metric-1";

    Metric metric = new Metric();
    metric.setMetric(metricName);
    Map<String, String> tags = Map.of("host", "h-1", "os", "linux", "deployment", "prod");
    metric.setTags(tags);
    elasticSearchService.saveMetricToES(tenantId, metric).block();

    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("metricName");
    filter.setFilterValue("xyz");
    criteria.setFilter(List.of(filter));
    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId+"axb", criteria);

    assertThat(metricDTOSResult.size()).isEqualTo(0);
  }

  @Test
  public void testSearchForMetricNames() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);

    final String metricName1 = RandomStringUtils.randomAlphanumeric(10);
    final String metricName2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName1);
    Map<String, String> tags1 = Map.of("host", "h-1", "os", "linux", "deployment", "prod");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName2);
    Map<String, String> tags2 = Map.of("host", "h-2", "os", "windows", "deployment", "dev");
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);
    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("metricName"));

    MetricDTO metricDTO1 = new MetricDTO(metricName1, null);
    MetricDTO metricDTO2 = new MetricDTO(metricName2, null);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchForMetricGroups() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphanumeric(10);

    final String metricGroup1 = RandomStringUtils.randomAlphanumeric(10);
    final String metricGroup2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName);
    Map<String, String> tags1 = Map.of("host", "h-2", "os", "windows", "deployment", "dev", "metricGroup", metricGroup1);
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName);
    Map<String, String> tags2 = Map.of("host", "h-1", "os", "linux", "deployment", "prod", "metricGroup", metricGroup2);
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);
    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("tags.metricGroup"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setTags(Map.of("metricGroup", metricGroup1));
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setTags(Map.of("metricGroup", metricGroup2));

    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }
  @Test
  public void testSearchForGetTagsWithMetricName() throws InterruptedException, IOException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);

    final String metricName1 = RandomStringUtils.randomAlphanumeric(10);
    final String metricName2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName1);
    Map<String, String> tags1 = Map.of("host", "h-1", "os", "linux", "deployment", "prod");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName2);
    Map<String, String> tags2 = Map.of("host", "h-2", "os", "windows", "deployment", "dev");
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);
    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("tags"));

    MetricDTO metricDTO1 = new MetricDTO(null, tags1);
    MetricDTO metricDTO2 = new MetricDTO(null, tags2);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchForGetTagsWithMetricGroup() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphanumeric(10);

    final String metricGroup1 = RandomStringUtils.randomAlphanumeric(10);
    final String metricGroup2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName);
    Map<String, String> tags1 = Map.of("host", "h-2", "os", "windows", "deployment", "dev", "metricGroup", metricGroup1);
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName);
    Map<String, String> tags2 = Map.of("host", "h-1", "os", "linux", "deployment", "prod", "metricGroup", metricGroup2);
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);
    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("tags"));

    MetricDTO metricDTO1 = new MetricDTO(null, tags1);
    MetricDTO metricDTO2 = new MetricDTO(null, tags2);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchForGetResources() throws InterruptedException, IOException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphanumeric(10);

    final String resource1 = RandomStringUtils.randomAlphanumeric(10);
    final String resource2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName);
    Map<String, String> tags1 = Map.of("resource", resource1, "os", "windows", "deployment", "dev");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName);
    Map<String, String> tags2 = Map.of("resource", resource2, "os", "linux", "deployment", "prod");
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);

    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("tags.resource"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setTags(Map.of("resource", resource1));
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setTags(Map.of("resource", resource2));
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchForMetricNamesWithResourceId() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName1 = "metric1";
    final String metricName2 = "metric2";

    final String resource = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName1);
    Map<String, String> tags1 = Map.of("resource", resource, "os", "windows", "deployment", "dev");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName2);
    Map<String, String> tags2 = Map.of("resource", resource, "os", "linux", "deployment", "prod");
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);

    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.resource");
    filter.setFilterValue(resource);
    criteria.setFilter(List.of(filter));
    criteria.setIncludeFields(List.of("metricName"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setMetricName(metricName1);
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setMetricName(metricName2);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchForMetricGroupsWithResourceId() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName = RandomStringUtils.randomAlphanumeric(10);
    final String resource = RandomStringUtils.randomAlphanumeric(10);

    final String metricGroups1 = RandomStringUtils.randomAlphanumeric(10);
    final String metricGroups2 = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName);
    Map<String, String> tags1 = Map.of("resource", resource, "os", "windows", "deployment", "dev", "metricGroup", metricGroups1);
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName);
    Map<String, String> tags2 = Map.of("resource", resource, "os", "linux", "deployment", "prod", "metricGroup", metricGroups2);
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();
    Thread.sleep(1000);

    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.resource");
    filter.setFilterValue(resource);
    criteria.setFilter(List.of(filter));
    criteria.setIncludeFields(List.of("tags.metricGroup"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setTags(Map.of("metricGroup", metricGroups1));
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setTags(Map.of("metricGroup", metricGroups2));
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchWithWildcard() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);

    final String metricName1 = "metric1";
    final String metricName2 = "metric1-test";
    final String metricName3 = "metric2-test";

    Metric metric1 = new Metric();
    metric1.setMetric(metricName1);
    Map<String, String> tags1 = Map.of("host", "h-1", "os", "linux", "deployment", "prod");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();

    Metric metric2 = new Metric();
    metric2.setMetric(metricName2);
    Map<String, String> tags2 = Map.of("host", "h-2", "os", "windows", "deployment", "dev");
    metric2.setTags(tags2);
    elasticSearchService.saveMetricToES(tenantId,metric2).block();

    Metric metric3 = new Metric();
    metric3.setMetric(metricName3);
    Map<String, String> tags3 = Map.of("host", "h-2", "os", "windows", "deployment", "dev");
    metric3.setTags(tags3);
    elasticSearchService.saveMetricToES(tenantId,metric3).block();
    Thread.sleep(1000);
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("metricName");
    filter.setFilterValue("metric1*");
    criteria.setFilter(List.of(filter));

    MetricDTO metricDTO1 = new MetricDTO(metricName1, tags1);
    MetricDTO metricDTO2 = new MetricDTO(metricName2, tags2);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1, metricDTO2);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }

  @Test
  public void testSearchForEmptyFilterKeyValue() throws IOException, InterruptedException {
    final String tenantId = RandomStringUtils.randomAlphanumeric(10);
    final String metricName1 = "metric1";

    final String resource = RandomStringUtils.randomAlphanumeric(10);

    Metric metric1 = new Metric();
    metric1.setMetric(metricName1);
    Map<String, String> tags1 = Map.of("resource", resource, "os", "windows", "deployment", "dev");
    metric1.setTags(tags1);
    elasticSearchService.saveMetricToES(tenantId,metric1).block();
    Thread.sleep(1000);

    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("");
    filter.setFilterValue("");
    criteria.setFilter(List.of(filter));
    criteria.setIncludeFields(List.of("metricName"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setMetricName(metricName1);
    Set<MetricDTO> metricDTOSet = Set.of(metricDTO1);
    List<MetricDTO> metricDTOSExpected = new ArrayList<>();
    metricDTOSExpected.addAll(metricDTOSet);

    List<MetricDTO> metricDTOSResult = elasticSearchService.search(tenantId, criteria);
    assertThat(metricDTOSResult.size()).isEqualTo(metricDTOSExpected.size());
    assertThat(metricDTOSResult).containsAll(metricDTOSExpected);
  }
}
