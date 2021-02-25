package com.rackspace.ceres.app.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.PutResponse;
import com.rackspace.ceres.app.model.TagFilter;
import com.rackspace.ceres.app.services.DataWriteService;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@ActiveProfiles(profiles = {"test", "ingest"})
@WebFluxTest(WriteController.class)
@Import(AppProperties.class)
public class WriteControllerTest {

  @MockBean
  DataWriteService dataWriteService;

  @Autowired
  WebTestClient webTestClient;

  @Autowired
  AppProperties appProperties;

  @Captor
  ArgumentCaptor<Flux<Tuple2<String, Metric>>> metrics;

  @Test
  public void testPutMetrics() {

    Metric metric = new Metric().setMetric("metricA").setTags(
        Collections.singletonMap("os", "linux")).setTimestamp(Instant.now()).setValue(123);

    when(dataWriteService.ingest(any())).thenReturn(Flux.just(metric));

    webTestClient.post().uri("/api/put").body(Flux.just(metric), Metric.class).exchange()
        .expectStatus().isNoContent();

    verify(dataWriteService).ingest(metrics.capture());

    //verify tenant is default when not present in header or tagsMap
    StepVerifier.create(metrics.getValue()).expectNext(Tuples.of("default", metric))
        .expectComplete().verify();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  public void testPutMetrics_WithParams() {

    Metric metric = new Metric().setMetric("metricA").setTags(
        Collections.singletonMap("os", "linux")).setTimestamp(Instant.now()).setValue(123);
    when(dataWriteService.ingest(any())).thenReturn(Flux.just(metric));

    //Tests for details and summary both true
    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/put").queryParam("details", true)
        .queryParam("summary", true).build()).bodyValue(List.of(metric))
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(PutResponse.class)
        .isEqualTo(new PutResponse().setSuccess(1).setFailed(0).setErrors(List.of()));
    verify(dataWriteService).ingest(metrics.capture());

    //verify tenant is same as present in header
    StepVerifier.create(metrics.getValue()).expectNext(Tuples.of("t-1", metric))
        .expectComplete().verify();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  public  void testPutMetric_JustSummaryParam() {
    Metric metric = new Metric().setMetric("metricA").setTags(
        Map.of("tenant", "t-1", "os", "linux")).setTimestamp(Instant.now()).setValue(123);
    when(dataWriteService.ingest(any())).thenReturn(Flux.just(metric));

    //Tests for when summary is true but details is false
    webTestClient.post().uri(uriBuilder -> uriBuilder.path("/api/put")
        .queryParam("summary", true).build()).body(Mono.just(metric), Metric.class)
        .exchange()
        .expectStatus().isOk().expectBody(PutResponse.class)
        .isEqualTo(new PutResponse().setSuccess(1).setFailed(0).setErrors(null));
    verify(dataWriteService).ingest(metrics.capture());

    //verify tenant is present in tagsMap of request body
    StepVerifier.create(metrics.getValue())
        .consumeNextWith(resp -> Assertions.assertThat(resp.getT1()).isEqualTo("t-1"))
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  public void testPutMetrics_ExcludeTags() {

    Map<String, String> tags = new HashMap<>();
    tags.put("os", "linux");
    tags.put("tenant", "t-1");
    tags.put("invalid-tag", "this-is-tag-with-invalid-length");

    Map<String, String> filteredTags = new HashMap<>();
    filteredTags.put("os", "linux");

    Metric filteredMetric = new Metric().setMetric("metricA").setTags(filteredTags).setTimestamp(Instant.now())
        .setValue(123);

    Metric metric = new Metric().setMetric("metricA").setTags(tags).setTimestamp(Instant.now())
        .setValue(123);

    when(dataWriteService.ingest(any())).thenReturn(Flux.just(filteredMetric));

    webTestClient.post().uri("/api/put").body(Flux.just(metric), Metric.class).exchange()
        .expectStatus().isNoContent();

    verify(dataWriteService).ingest(metrics.capture());

    StepVerifier.create(metrics.getValue())
        .assertNext(t -> {
          assertThat(t.getT1()).isEqualTo("t-1");
          assertThat(t.getT2().getTags().size()).isEqualTo(1);
          assertThat(t.getT2().getTags().get("invalid-tag")).isNull();
        })
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }

  @Test
  public void testPutMetrics_TruncateTags() {

    //Setting the Tag filter to truncate instead of excluding
    appProperties.setTagFilter(TagFilter.TRUNCATE);

    Map<String, String> tags = new HashMap<>();
    tags.put("os", "linux");
    tags.put("tenant", "t-1");
    tags.put("invalid-tag", "this-is-tag-with-invalid-length");

    Map<String, String> filteredTags = new HashMap<>();
    filteredTags.put("os", "linux");

    Metric filteredMetric = new Metric().setMetric("metricA").setTags(filteredTags).setTimestamp(Instant.now())
        .setValue(123);

    Metric metric = new Metric().setMetric("metricA").setTags(tags).setTimestamp(Instant.now())
        .setValue(123);

    when(dataWriteService.ingest(any())).thenReturn(Flux.just(filteredMetric));

    webTestClient.post().uri("/api/put").body(Flux.just(metric), Metric.class).exchange()
        .expectStatus().isNoContent();

    verify(dataWriteService).ingest(metrics.capture());

    StepVerifier.create(metrics.getValue())
        .assertNext(t -> {
          assertThat(t.getT1()).isEqualTo("t-1");
          assertThat(t.getT2().getTags().size()).isEqualTo(2);
          assertThat(t.getT2().getTags().get("invalid-tag")).isEqualTo("this-is-ta");
        })
        .verifyComplete();

    verifyNoMoreInteractions(dataWriteService);
  }
}
