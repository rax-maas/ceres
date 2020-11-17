package com.rackspace.ceres.app.web;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.model.QueryResult;
import com.rackspace.ceres.app.services.QueryService;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
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

    Map<String, String> queryTags = new HashMap<>();
    queryTags.put("os", "linux");
    queryTags.put("deployment", "dev");
    queryTags.put("host", "h-1");

    Map<Instant, Double> values = new HashMap<>();
    values.put(Instant.now(), 111.0);

    List<QueryResult> queryResults = new ArrayList<>();
    queryResults.add(new QueryResult().setMetricName("cpu-idle").setTags(queryTags).setTenant("t-1")
        .setValues(values));

    when(queryService.queryRaw(anyString(), anyString(), any(), any(), any()))
        .thenReturn(Flux.just(queryResults.get(0)));

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

    Map<String, String> queryTags = new HashMap<>();
    queryTags.put("os", "linux");
    queryTags.put("deployment", "dev");
    queryTags.put("host", "h-1");

    Map<Instant, Double> values = new HashMap<>();
    values.put(Instant.now(), 111.0);

    List<QueryResult> queryResults = new ArrayList<>();
    queryResults.add(new QueryResult().setMetricName("cpu-idle").setTags(queryTags).setTenant("t-1")
        .setValues(values));

    when(queryService.queryDownsampled(anyString(), anyString(), any(), any(), any(), any(), any()))
        .thenReturn(Flux.just(queryResults.get(0)));

    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("tenant", "t-1")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .queryParam("end", "1605117336")
            .queryParam("aggregator", "min")
            .queryParam("granularity", "pt1m")
            .build())
        .exchange().expectStatus().isOk()
        .expectBodyList(QueryResult.class).isEqualTo(queryResults);
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
        .jsonPath("$.status").isEqualTo(500)
        .jsonPath("$.message").isEqualTo("granularity is required when using aggregator");

  }
}
