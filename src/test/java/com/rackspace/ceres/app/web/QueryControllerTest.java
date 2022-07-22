package com.rackspace.ceres.app.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.Aggregator;
import com.rackspace.ceres.app.model.Metadata;
import com.rackspace.ceres.app.model.QueryData;
import com.rackspace.ceres.app.model.QueryResult;
import com.rackspace.ceres.app.services.QueryService;
import com.rackspace.ceres.app.validation.RequestValidator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebFlux;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@ActiveProfiles(profiles = {"test", "query"})
@SpringBootTest(classes = {QueryController.class, AppProperties.class,
    RestWebExceptionHandler.class,
    DownsampleProperties.class, SimpleMeterRegistry.class})
@AutoConfigureWebTestClient
@AutoConfigureWebFlux
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class QueryControllerTest {

  @MockBean
  QueryService queryService;

  @Autowired
  MeterRegistry meterRegistry;

  @Autowired
  private WebTestClient webTestClient;

  @MockBean
  RequestValidator requestValidator;

  @Test
  public void testQueryApiWithMetricName() {
    Map<String, String> queryTags = Map
        .of("os", "linux", "deployment", "dev", "host", "h-1");

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult().setData(new QueryData().setMetricName("cpu-idle").setTags(queryTags).setTenant("t-1")
            .setValues(values)).setMetadata(new Metadata().setAggregator(Aggregator.raw)));

    when(queryService.queryRaw(anyString(), anyString(), eq(null), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    Flux<QueryResult> result = webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .returnResult(QueryResult.class).getResponseBody();

    double count = meterRegistry.get("ceres.query").tag("type", "raw").counter().count();
    assertThat(count).isEqualTo(1.0);

    StepVerifier.create(result).assertNext(queryResult -> {
      assertThat(meterRegistry.get("ceres.query").tag("type", "raw").counter().count()).isEqualTo(1);
      assertThat(queryResult.getData()).isEqualTo(queryResults.get(0).getData());
      assertThat(queryResult.getMetadata().getAggregator()).isEqualTo(Aggregator.raw);
    }).verifyComplete();
  }

  @Test
  public void testQueryApiWithMetricGroup() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Map<String, String> queryTags = Map
        .of("os", "linux", "deployment", "dev", "host", "h-1", "metricGroup", metricGroup);

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult().setData(new QueryData().setMetricName("cpu-idle").setTags(queryTags).setTenant("t-1")
            .setValues(values)).setMetadata(new Metadata().setAggregator(Aggregator.raw)));

    when(queryService.queryRaw(anyString(), eq(null), anyString(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    Flux<QueryResult> result = webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricGroup", metricGroup)
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .returnResult(QueryResult.class).getResponseBody();

    StepVerifier.create(result).assertNext(queryResult -> {
      assertThat(queryResult.getData()).isEqualTo(queryResults.get(0).getData());
      assertThat(queryResult.getMetadata().getAggregator()).isEqualTo(Aggregator.raw);
      assertThat(queryResult.getData().getTags().get("metricGroup")).isEqualTo(metricGroup);
    }).verifyComplete();
  }

  @Test
  public void testQueryApiWithAggregator() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Map<String, String> queryTags = Map
        .of("os", "linux", "deployment", "dev", "host", "h-1","metricGroup", metricGroup);

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult()
            .setData(
                new QueryData()
                    .setMetricName("cpu-idle")
                    .setTags(queryTags)
                    .setTenant("t-1")
                    .setValues(values))
            .setMetadata(
                new Metadata()
                    .setAggregator(Aggregator.min)
                    .setGranularity(Duration.ofMinutes(1))
                    .setStartTime(Instant.ofEpochSecond(1605611015))
                    .setEndTime(Instant.ofEpochSecond(1605697439))));

    when(queryService.queryDownsampled(anyString(), anyString(), eq(null), any(), any(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    Flux<QueryResult> result = webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux,deployment=dev,host=h-1,metricGroup="+metricGroup)
            .queryParam("start", "1605611015")
            .queryParam("end", "1605697439")
            .queryParam("aggregator", "min")
            .queryParam("granularity", "pt1m")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .returnResult(QueryResult.class).getResponseBody();

    StepVerifier.create(result).assertNext(queryResult -> {
      assertThat(queryResult.getData()).isEqualTo(queryResults.get(0).getData());
      assertThat(queryResult.getMetadata()).isEqualTo(queryResults.get(0).getMetadata());
      assertThat(queryResult.getData().getTags().get("metricGroup")).isEqualTo(metricGroup);
    }).verifyComplete();

    verify(queryService)
        .queryDownsampled("t-1", "cpu-idle", null, Aggregator.min, Duration.ofMinutes(1), queryTags,
            Instant.ofEpochSecond(1605611015), Instant.ofEpochSecond(1605697439));

    verifyNoMoreInteractions(queryService);
  }

  @Test
  public void testQueryApiWithAggregatorWithMetricGroup() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Map<String, String> queryTags = Map
        .of("os", "linux", "deployment", "dev", "host", "h-1", "metricGroup", metricGroup);

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult()
            .setData(
                new QueryData()
                    .setMetricName("cpu-idle")
                    .setTags(queryTags)
                    .setTenant("t-1")
                    .setValues(values))
            .setMetadata(
                new Metadata()
                    .setAggregator(Aggregator.min)
                    .setGranularity(Duration.ofMinutes(1))
                    .setStartTime(Instant.ofEpochSecond(1605611015))
                    .setEndTime(Instant.ofEpochSecond(1605697439))));

    when(queryService.queryDownsampled(anyString(), eq(null), anyString(), any(), any(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    Flux<QueryResult> result = webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricGroup", metricGroup)
            .queryParam("tag", "os=linux,deployment=dev,host=h-1,metricGroup="+metricGroup)
            .queryParam("start", "1605611015")
            .queryParam("end", "1605697439")
            .queryParam("aggregator", "min")
            .queryParam("granularity", "pt1m")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .returnResult(QueryResult.class).getResponseBody();

    StepVerifier.create(result).assertNext(queryResult -> {
      assertThat(queryResult.getData()).isEqualTo(queryResults.get(0).getData());
      assertThat(queryResult.getMetadata()).isEqualTo(queryResults.get(0).getMetadata());
      assertThat(queryResult.getData().getTags().get("metricGroup")).isEqualTo(queryResults.get(0).getData().getTags().get("metricGroup"));
    }).verifyComplete();

    verify(queryService)
        .queryDownsampled("t-1", null, metricGroup, Aggregator.min, Duration.ofMinutes(1), queryTags,
            Instant.ofEpochSecond(1605611015), Instant.ofEpochSecond(1605697439));

    verifyNoMoreInteractions(queryService);
  }

  @Test
  public void testQueryApiWithAggregatorAndMissingGranularity() {

    Map<String, String> queryTags = Map.of("os", "linux", "deployment", "dev", "host", "h-1");

    Map<Instant, Double> values = Map.of(Instant.now(), 111.0);

    List<QueryResult> queryResults = List
        .of(new QueryResult()
            .setData(
                new QueryData()
                    .setMetricName("cpu-idle")
                    .setTags(queryTags)
                    .setTenant("t-1")
                    .setValues(values))
            .setMetadata(
                new Metadata()
                    .setAggregator(Aggregator.min)
                    .setGranularity(Duration.ofMinutes(1))
                    .setStartTime(Instant.ofEpochSecond(1605611015))
                    .setEndTime(Instant.ofEpochSecond(1605697439))));

    when(queryService.queryDownsampled(anyString(), anyString(), eq(null), any(), any(), any(), any(), any()))
        .thenReturn(Flux.fromIterable(queryResults));

    Flux<QueryResult> result = webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux,deployment=dev,host=h-1,")
            .queryParam("start", "1605611015")
            .queryParam("end", "1605697439")
            .queryParam("aggregator", "max")
            .build())
        .header("X-Tenant", "t-1")
        .exchange().expectStatus().isOk()
        .returnResult(QueryResult.class).getResponseBody();

    assertThat(result).isNotNull();
    double count = ((CumulativeCounter) meterRegistry.getMeters().get(0)).count();
    assertThat(count).isEqualTo(1.0);

    StepVerifier.create(result).assertNext(queryResult -> {
      assertThat(queryResult.getData()).isEqualTo(queryResults.get(0).getData());
      assertThat(queryResult.getMetadata()).isEqualTo(queryResults.get(0).getMetadata());
    }).verifyComplete();

    verify(queryService)
        .queryDownsampled("t-1", "cpu-idle", null, Aggregator.max, Duration.ofHours(1), queryTags,
            Instant.ofEpochSecond(1605611015), Instant.ofEpochSecond(1605697439));

    verifyNoMoreInteractions(queryService);
  }

  @Test
  public void testQueryApiWithNoTenantInHeaderAndParam() {

    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .exchange().expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400);

    verifyNoInteractions(queryService);
  }

  @Test
  public void testQueryApiWithMetricNameAndMetricGroup() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("metricGroup", metricGroup)
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .exchange().expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400);
  }

  @Test
  public void testQueryApiWithNoMetricNameAndMetricGroup() {
    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("tag", "os=linux")
            .queryParam("start", "1d-ago")
            .build())
        .exchange().expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400);

    verifyNoInteractions(queryService);
  }

  @Test
  public void testQueryApiWithOutStart() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("metricGroup", metricGroup)
            .queryParam("tag", "os=linux")
            .build())
        .exchange().expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400);
    verifyNoInteractions(queryService);
  }

  @Test
  public void testQueryApiWithInvalidAggregator() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("metricGroup", metricGroup)
            .queryParam("tag", "os=linux")
            .queryParam("aggregator", "test")
            .build())
        .exchange().expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400);
    verifyNoInteractions(queryService);
  }

  @Test
  public void testQueryApiWithInvalidTagName() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    webTestClient.get()
        .uri(uriBuilder -> uriBuilder.path("/api/query")
            .queryParam("metricName", "cpu-idle")
            .queryParam("metricGroup", metricGroup)
            .queryParam("tag", "test")
            .queryParam("aggregator", "min")
            .build())
        .exchange().expectStatus().isBadRequest()
        .expectBody()
        .jsonPath("$.status").isEqualTo(400);
    verifyNoInteractions(queryService);
  }
}
