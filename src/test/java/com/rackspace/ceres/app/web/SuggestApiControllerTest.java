/*
 * Copyright 2021 Rackspace US, Inc.
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
 */

package com.rackspace.ceres.app.web;

import static org.mockito.Mockito.when;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.SuggestTypeEnumConverter;
import com.rackspace.ceres.app.services.SuggestApiService;
import java.util.Collections;
import java.util.List;
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

@ActiveProfiles(profiles = {"test"})
@SpringBootTest(classes = {SuggestApiController.class, AppProperties.class,
    SuggestTypeEnumConverter.class})
@AutoConfigureWebTestClient
@AutoConfigureWebFlux
public class SuggestApiControllerTest {

  @MockBean
  SuggestApiService suggestApiService;

  @Autowired
  WebTestClient webTestClient;

  @Test
  public void testGetSuggestionsMetricNames() {
    when(suggestApiService.suggestMetricNames("t-1", "cpu", 10))
        .thenReturn(Mono.just(List.of("cpu_idle", "cpu_busy")));

    webTestClient.get().uri(uriBuilder -> uriBuilder.path("/api/suggest")
        .queryParam("type", "metrics")
        .queryParam("q", "cpu")
        .queryParam("max", 10)
        .build())
        .header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(List.class)
        .value(val -> Assertions.assertThat(val).contains("cpu_idle", "cpu_busy"));
  }

  @Test
  public void testGetSuggestionsTagKeys() {
    when(suggestApiService.suggestTagKeys("t-1", "os", 10))
        .thenReturn(Mono.just(List.of("os", "osx")));

    webTestClient.get().uri(uriBuilder -> uriBuilder.path("/api/suggest")
        .queryParam("type", "tagk")
        .queryParam("q", "os")
        .queryParam("max", 10)
        .build()).header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(List.class)
        .value(val -> Assertions.assertThat(val).contains("os", "osx"));
  }

  @Test
  public void testGetSuggestionsTagValues() {
    when(suggestApiService.suggestTagValues("t-1", "h-", 10))
        .thenReturn(Mono.just(List.of("h-1", "h-2", "h-3")));

    webTestClient.get().uri(uriBuilder -> uriBuilder.path("/api/suggest")
        .queryParam("type", "tagv")
        .queryParam("q", "h-")
        .queryParam("max", 10)
        .build()).header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(List.class)
        .value(val -> Assertions.assertThat(val).contains("h-1", "h-2","h-3"));
  }

  @Test
  public void testGetSuggestionsInvalidType() {
    webTestClient.get().uri(uriBuilder -> uriBuilder.path("/api/suggest")
        .queryParam("type", "invalid")
        .queryParam("q", "h-")
        .queryParam("max", 10)
        .build()).header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isBadRequest();
  }

  @Test
  public void testGetSuggestionsMax0() {
    when(suggestApiService.suggestTagValues("t-1", "h-", 0))
        .thenReturn(Mono.just(Collections.emptyList()));

    webTestClient.get().uri(uriBuilder -> uriBuilder.path("/api/suggest")
        .queryParam("type", "tagv")
        .queryParam("q", "h-")
        .queryParam("max", 0)
        .build()).header("X-Tenant", "t-1")
        .exchange()
        .expectStatus().isOk().expectBody(List.class)
        .isEqualTo(Collections.emptyList());
  }

  @Test
  public void testGetSuggestionsTenantHeaderMissing() {
    when(suggestApiService.suggestTagValues("t-1", "h-", 0))
        .thenReturn(Mono.just(Collections.emptyList()));

    webTestClient.get().uri(uriBuilder -> uriBuilder.path("/api/suggest")
        .queryParam("type", "tagv")
        .queryParam("q", "h-")
        .queryParam("max", 10)
        .build())
        .exchange()
        .expectStatus().isBadRequest();
  }
}
