package com.rackspace.ceres.app.web;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.services.MetadataService;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@ActiveProfiles("test")
@WebFluxTest(MetadataController.class)
public class MetadataControllerTest {

  @MockBean
  MetadataService metadataService;

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
        uriBuilder -> uriBuilder.path("/api/metadata/metricNames").queryParam("tenant", "t-1")
            .build())
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("metric-1", "metric-2"));

    verify(metadataService).getMetricNames("t-1");
    verifyNoMoreInteractions(metadataService);
  }

  @Test
  public void testGetTagKeys() {

    when(metadataService.getTagKeys("t-1", "metric-1"))
        .thenReturn(Mono.just(List.of("os", "host")));

    webTestClient.get().uri(
        uriBuilder -> uriBuilder.path("/api/metadata/tagKeys").queryParam("tenant", "t-1")
            .queryParam("metricName", "metric-1").build())
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
        uriBuilder -> uriBuilder.path("/api/metadata/tagValues").queryParam("tenant", "t-1")
            .queryParam("metricName", "metric-1").queryParam("tagKey", "os").build())
        .exchange().expectStatus().isOk()
        .expectBody(List.class)
        .isEqualTo(List.of("linux"));

    verify(metadataService).getTagValues("t-1", "metric-1", "os");
    verifyNoMoreInteractions(metadataService);
  }
}
