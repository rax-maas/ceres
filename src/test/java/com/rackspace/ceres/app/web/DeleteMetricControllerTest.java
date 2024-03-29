package com.rackspace.ceres.app.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.services.MetricDeletionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebFlux;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@ActiveProfiles(profiles = {"test", "admin"})
@SpringBootTest(classes = {DeleteMetricController.class, AppProperties.class,
    RestWebExceptionHandler.class,
    DownsampleProperties.class})
@AutoConfigureWebTestClient
@AutoConfigureWebFlux
public class DeleteMetricControllerTest {

  @MockBean
  MetricDeletionService metricDeletionService;

  @Autowired
  private WebTestClient webTestClient;

  @Test
  public void testDeleteMetricByTenantId() {
    when(metricDeletionService.deleteMetricsByTenantId(anyString(), any(), any()))
        .thenReturn(Mono.empty());

    webTestClient.delete()
        .uri(uriBuilder -> uriBuilder.path("/api/metric")
            .queryParam("start", "1d-ago")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk();
  }

  @Test
  public void testDeleteMetricByTenantIdAndWithoutStartTime() {
    when(metricDeletionService.deleteMetricsByTenantId(anyString(), any(), any()))
        .thenReturn(Mono.empty());

    webTestClient.delete()
        .uri(uriBuilder -> uriBuilder.path("/api/metric")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk();
  }

  @Test
  public void testDeleteMetricByMetricName() {
    when(metricDeletionService.deleteMetricsByMetricName(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());

    webTestClient.delete()
        .uri(uriBuilder -> uriBuilder.path("/api/metric")
            .queryParam("metricName", "cpu-idle")
            .queryParam("start", "1d-ago")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk();
  }

  @Test
  public void testDeleteMetricByMetricNameAndTag() {
    when(metricDeletionService.deleteMetricsByMetricNameAndTag(anyString(), anyString(), any(), any(), any()))
        .thenReturn(Mono.empty());

    webTestClient.delete()
        .uri(uriBuilder -> uriBuilder.path("/api/metric")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk();
  }

  @Test
  public void testDeleteMetricByMetricGroup() {
    when(metricDeletionService.deleteMetricsByMetricGroup(anyString(), anyString(), any(), any()))
        .thenReturn(Mono.empty());

    webTestClient.delete()
        .uri(uriBuilder -> uriBuilder.path("/api/metric")
            .queryParam("metricGroup", "cpu")
            .queryParam("start", "1d-ago")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk();
  }
}
