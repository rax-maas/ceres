package com.rackspace.ceres.app.web;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.Criteria;
import com.rackspace.ceres.app.model.Filter;
import com.rackspace.ceres.app.model.MetricDTO;
import com.rackspace.ceres.app.model.TagsResponse;
import com.rackspace.ceres.app.services.ElasticSearchService;
import com.rackspace.ceres.app.services.MetadataService;
import com.rackspace.ceres.app.validation.RequestValidator;
import io.lettuce.core.RedisBusyException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebFlux;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@ActiveProfiles(profiles = {"test", "query", "dev"})
@SpringBootTest(classes = {MetadataController.class, AppProperties.class,
    RequestValidator.class})
@AutoConfigureWebTestClient
@AutoConfigureWebFlux
public class MetadataControllerTest {

  @MockBean
  MetadataService metadataService;

  @MockBean
  ElasticSearchService elasticSearchService;

  @Autowired
  RequestValidator requestValidator;

  @Autowired
  WebTestClient webTestClient;

  @Test
  public void testGetTenants() {

    when(metadataService.getTenants()).thenReturn(Mono.just(List.of("t-1", "t-2")));

    webTestClient.get().uri("/api/metadata/tenants").exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("t-1", "t-2"));

    verify(metadataService).getTenants();
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetMetricNames() {

    when(metadataService.getMetricNames("t-1"))
        .thenReturn(Mono.just(List.of("metric-1", "metric-2")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/metricNames")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("metric-1", "metric-2"));

    verify(metadataService).getMetricNames("t-1");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetMetricGroups() {
    when(metadataService.getMetricGroups("t-1"))
        .thenReturn(Mono.just(List.of("group-1", "group-2")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/metricGroups")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("group-1", "group-2"));

    verify(metadataService).getMetricGroups("t-1");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetTagKeys() {

    when(metadataService.getTagKeys("t-1", "metric-1"))
        .thenReturn(Mono.just(List.of("os", "host")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/tagKeys")
            .queryParam("metricName", "metric-1").build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("os", "host"));

    verify(metadataService).getTagKeys("t-1", "metric-1");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetTagValues() {

    when(metadataService.getTagValues("t-1", "metric-1", "os"))
        .thenReturn(Mono.just(List.of("linux")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/tagValues")
            .queryParam("metricName", "metric-1").queryParam("tagKey", "os").build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("linux"));

    verify(metadataService).getTagValues("t-1", "metric-1", "os");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetTagWithMetricName() {
    Map<String, String> tags = Map.of("os", "linux", "host", "h1");
    when(metadataService.getTags("t-1", "metric-1", null))
        .thenReturn(Mono.just(new TagsResponse().setMetric("metric-1").setTenantId("t-1")
            .setTags(tags)));
    TagsResponse tagsResponse = new TagsResponse().setTenantId("t-1")
        .setMetric("metric-1").setTags(tags);

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/tags")
            .queryParam("metricName", "metric-1").build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(TagsResponse.class)
        .isEqualTo(tagsResponse);

    verify(metadataService).getTags("t-1", "metric-1", null);
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetTagWithMetricGroup() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Map<String, String> tags = Map.of("os", "linux", "host", "h1");
    when(metadataService.getTags("t-1", null, metricGroup))
        .thenReturn(Mono.just(new TagsResponse().setMetricGroup(metricGroup).setTenantId("t-1")
            .setTags(tags)));
    TagsResponse tagsResponse = new TagsResponse().setTenantId("t-1")
        .setMetricGroup(metricGroup).setTags(tags);

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/tags")
            .queryParam("metricGroup", metricGroup).build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(TagsResponse.class)
        .isEqualTo(tagsResponse);

    verify(metadataService).getTags("t-1", null, metricGroup);
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetDevices() {
    when(metadataService.getDevices("t-1"))
        .thenReturn(Mono.just(List.of("device1", "device2")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/devices").build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .value(val -> Assertions.assertThat(val).contains("device1", "device2"));

    verify(metadataService).getDevices("t-1");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetMetricNamesWithDeviceId() {

    when(metadataService.getMetricNamesFromDevice("t-1", "device-1"))
        .thenReturn(Mono.just(List.of("metric-1", "metric-2")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/metricNames")
            .queryParam("device", "device-1").build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("metric-1", "metric-2"));

    verify(metadataService).getMetricNamesFromDevice("t-1", "device-1");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetTenantsForEServerError() {
    when(metadataService.getTenants()).thenReturn(Mono.error(new RedisBusyException(
        "Service encountered an unexpected condition which prevented it from fulfilling the request.")));

    webTestClient.get().uri("/api/metadata/tenants").exchange().expectStatus().is5xxServerError()
        .expectBody()
        .jsonPath("$.message").isEqualTo(
            "Service encountered an unexpected condition which prevented it from fulfilling the request.")
        .jsonPath("$.path").isEqualTo("/api/metadata/tenants")
        .jsonPath("$.error").isEqualTo("Internal Server Error");

    verify(metadataService).getTenants();
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testSearchForMetricNames() throws IOException {
    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("metricNames"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setMetricName("metric-1");
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setMetricName("metric-1");

    List<MetricDTO> metricDTOS = List.of(metricDTO1, metricDTO2);
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }

  @Test
  public void testSearchForMetricGroups() throws IOException {
    Criteria criteria = new Criteria();
    criteria.setIncludeFields(List.of("tags.metricGroup"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setTags(Map
        .of("metricGroup","group-1"));
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setTags(Map
        .of("metricGroup","group-2"));

    List<MetricDTO> metricDTOS = List.of(metricDTO1, metricDTO2);
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }
  @Test
  public void testSearchForGetTagsWithMetricName() throws IOException {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("metricName");
    filter.setFilterValue(metricName);
    criteria.setFilter(List.of(filter));

    Map<String, String> tags = Map.of("os", "linux", "host", "h1","metricGroup", "group-1");

    List<MetricDTO> metricDTOS = List.of(new MetricDTO(metricName, tags));
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }

  @Test
  public void testSearchForGetTagsWithMetricGroup() throws IOException {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.metricGroup");
    filter.setFilterValue(metricGroup);
    criteria.setFilter(List.of(filter));

    Map<String, String> tags = Map.of("os", "linux", "host", "h1","metricGroup", metricGroup);

    List<MetricDTO> metricDTOS = List.of(new MetricDTO(metricName, tags));
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }

  @Test
  public void testSearchForGetDevices() throws IOException {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String device = RandomStringUtils.randomAlphabetic(5);
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.device");
    filter.setFilterValue(device);
    criteria.setFilter(List.of(filter));

    Map<String, String> tags = Map.of("os", "linux", "host", "h1","device", device);

    List<MetricDTO> metricDTOS = List.of(new MetricDTO(metricName, tags));
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }

  @Test
  public void testSearchForMetricNamesWithDeviceId() throws IOException {
    final String device = RandomStringUtils.randomAlphabetic(5);
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.device");
    filter.setFilterValue(device);
    criteria.setFilter(List.of(filter));
    criteria.setIncludeFields(List.of("metricNames"));

    MetricDTO metricDTO1 = new MetricDTO();
    metricDTO1.setMetricName("metric-1");
    MetricDTO metricDTO2 = new MetricDTO();
    metricDTO2.setMetricName("metric-1");

    List<MetricDTO> metricDTOS = List.of(metricDTO1, metricDTO2);
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }

  @Test
  public void testSearchForMetricGroupsWithDeviceId() throws IOException {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final String device = RandomStringUtils.randomAlphabetic(5);
    Criteria criteria = new Criteria();
    Filter filter = new Filter();
    filter.setFilterKey("tags.device");
    filter.setFilterValue(device);
    criteria.setFilter(List.of(filter));
    criteria.setIncludeFields(List.of("tags.metricGroup"));

    Map<String, String> tags = Map.of("os", "linux", "host", "h1","metricGroup", metricGroup);

    MetricDTO metricDTO = new MetricDTO();
    metricDTO.setTags(tags);
    List<MetricDTO> metricDTOS = List.of(metricDTO);
    String jsonResponse = new ObjectMapper().writeValueAsString(metricDTOS);
    when(elasticSearchService.search("t-1",criteria))
        .thenReturn(metricDTOS);

    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/metadata/search")
            .build()).bodyValue(criteria)
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(String.class)
        .isEqualTo(jsonResponse);
  }
}
