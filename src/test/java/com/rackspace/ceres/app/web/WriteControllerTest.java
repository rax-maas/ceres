package com.rackspace.ceres.app.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.PutResponse;
import com.rackspace.ceres.app.services.DataWriteService;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

@ActiveProfiles("test")
@WebFluxTest(WriteController.class)
@Import(AppProperties.class)
public class WriteControllerTest {

  @MockBean
  DataWriteService dataWriteService;

  @Autowired
  WebTestClient webTestClient;

  @Test
  public void testPutMetrics() {

    Metric metric = new Metric().setMetric("metricA").setTags(
        Collections.singletonMap("os", "linux")).setTimestamp(Instant.now()).setValue(123);
    when(dataWriteService.ingest(any())).thenReturn(Flux.just(metric));

    webTestClient.post().uri("/api/put").body(Flux.just(metric), Metric.class).exchange()
        .expectStatus().isNoContent();
  }

  @Test
  public void testPutMetrics_WithParams() {

    Metric metric = new Metric().setMetric("metricA").setTags(
        Collections.singletonMap("os", "linux")).setTimestamp(Instant.now()).setValue(123);
    when(dataWriteService.ingest(any())).thenReturn(Flux.just(metric));

    //Tests for details and summary both true
    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/put").queryParam("details", true)
        .queryParam("summary", true).build()).body(Flux.just(metric), Metric.class)
        .exchange()
        .expectStatus().isOk().expectBody(PutResponse.class)
        .isEqualTo(new PutResponse().setSuccess(1).setFailed(0).setErrors(List.of()));

    //Tests for when summary is true but details is false
    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/put")
        .queryParam("summary", true).build()).body(Flux.just(metric), Metric.class)
        .exchange()
        .expectStatus().isOk().expectBody(PutResponse.class)
        .isEqualTo(new PutResponse().setSuccess(1).setFailed(0).setErrors(null));
  }
}
