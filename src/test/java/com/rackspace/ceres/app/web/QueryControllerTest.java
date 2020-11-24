package com.rackspace.ceres.app.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.model.QueryResult;
import com.rackspace.ceres.app.services.QueryService;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;

@ActiveProfiles("test")
@WebFluxTest(QueryController.class)
public class QueryControllerTest {

  @MockBean
  QueryService queryService;

  @Autowired
  private WebTestClient webTestClient;

  @Test
  public void testQueryApi() {

    Map<String, String> queryTags = Map.of("os", "linux", "deployment", "dev", "host", "h-1");

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult().setMetricName("cpu-idle").setTags(queryTags).setTenant("t-1")
            .setValues(values));

    when(queryService.queryRaw(anyString(), anyString(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("tenant", "t-1")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .exchange().expectStatus().isOk()
        .expectBodyList(QueryResult.class).isEqualTo(queryResults);
  }

  @Test
  public void testQueryApiWithAggregator() {

    Map<String, String> queryTags = Map.of("os", "linux", "deployment", "dev", "host", "h-1");

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult().setMetricName("cpu-idle").setTags(queryTags).setTenant("t-1")
            .setValues(values));

    when(queryService.queryDownsampled(anyString(), anyString(), any(), any(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("tenant", "t-1")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux,deployment=dev,host=h-1,")
            .queryParam("start", "1605611015")
            .queryParam("end", "1605697439")
            .queryParam("aggregator", "min")
            .queryParam("granularity", "pt1m")
            .build())
        .exchange().expectStatus().isOk()
        .expectBodyList(QueryResult.class).isEqualTo(queryResults);

    verify(queryService)
        .queryDownsampled("t-1", "cpu-idle", Aggregator.min, Duration.ofMinutes(1), queryTags,
            Instant.ofEpochSecond(1605611015), Instant.ofEpochSecond(1605697439));

    verifyNoMoreInteractions(queryService);
  }

  @Test
  public void testQueryApiWithAggregatorAndMissingGranularity() {

    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("tenant", "t-1")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .queryParam("end", "1605117336")
            .queryParam("aggregator", "min")
            .build())
        .exchange().expectStatus().is5xxServerError()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400)
        .jsonPath("$.message").isEqualTo("granularity is required when using aggregator")
        .jsonPath("$.exception").isEqualTo(IllegalArgumentException.class.getName());

    verifyNoInteractions(queryService);
  }
}
