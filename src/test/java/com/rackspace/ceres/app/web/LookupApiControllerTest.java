package com.rackspace.ceres.app.web;

import com.rackspace.ceres.app.model.LookupResult;
import com.rackspace.ceres.app.model.MetricNameAndMultiTags;
import com.rackspace.ceres.app.services.MetadataService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebFlux;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

@ActiveProfiles(profiles = {"test", "query", "dev"})
@SpringBootTest(classes = {LookupApiController.class})
@AutoConfigureWebTestClient
@AutoConfigureWebFlux
public class LookupApiControllerTest {

    @MockBean
    MetadataService metadataService;

    @Autowired
    WebTestClient webTestClient;

    @Test
    public void testSimpleMetric() {
        when(metadataService.getTagKeys("t-1", "cpu_active"))
                .thenReturn(Mono.just(List.of("os", "host")));
        when(metadataService.getTagValues("t-1", "cpu_active", "os"))
                .thenReturn(Mono.just(List.of("linux")));
        when(metadataService.getTagValues("t-1", "cpu_active", "host"))
                .thenReturn(Mono.just(List.of("h-1", "h-2")));
        when(metadataService.getMetricNameAndTags("cpu_active"))
                .thenReturn(new MetricNameAndMultiTags()
                        .setMetricName("cpu_active")
                        .setTags(new ArrayList<>()));

        webTestClient.get().uri(
                uriBuilder -> uriBuilder.path("/api/search/lookup")
                        .queryParam("m", "cpu_active")
                        .queryParam("limit", 2).build())
                .header("X-Tenant", "t-1")
                .header("Content-Type", "application/json")
                .exchange().expectStatus().isOk()
                .expectBody(LookupResult.class).consumeWith(response -> {

            assertThat(response.getResponseBody().getType()).isEqualTo("LOOKUP");
            assertThat(response.getResponseBody().getMetric()).isEqualTo("cpu_active");
            assertThat(response.getResponseBody().getLimit()).isEqualTo(2);
            assertThat(response.getResponseBody().getResults().size()).isEqualTo(2);

            response.getResponseBody().getResults().forEach(seriesData -> {
                seriesData.getTags().entrySet().stream().forEach(s -> {
                            assertTrue(
                                    // The order of the results are not deterministic
                                    (s.getKey().equals("os") && s.getValue().equals("linux")) ||
                                            (s.getKey().equals("host") && s.getValue().equals("h-1")) ||
                                            (s.getKey().equals("host") && s.getValue().equals("h-2"))
                            );
                        }
                );
            });
        });

        verify(metadataService).getTagKeys("t-1", "cpu_active");
        verify(metadataService).getTagValues("t-1", "cpu_active", "os");
        verify(metadataService).getTagValues("t-1", "cpu_active", "host");
        verify(metadataService).getMetricNameAndTags("cpu_active");
        verifyNoMoreInteractions(metadataService);
    }

    @Test
    public void testSimpleMetricNoLimit() {
        when(metadataService.getTagKeys("t-1", "cpu_active"))
                .thenReturn(Mono.just(List.of("os", "host")));
        when(metadataService.getTagValues("t-1", "cpu_active", "os"))
                .thenReturn(Mono.just(List.of("linux")));
        when(metadataService.getTagValues("t-1", "cpu_active", "host"))
                .thenReturn(Mono.just(List.of("h-1", "h-2")));
        when(metadataService.getMetricNameAndTags("cpu_active"))
                .thenReturn(new MetricNameAndMultiTags()
                        .setMetricName("cpu_active")
                        .setTags(new ArrayList<>()));

        webTestClient.get().uri(
                uriBuilder -> uriBuilder.path("/api/search/lookup")
                        .queryParam("m", "cpu_active").build())
                .header("X-Tenant", "t-1")
                .header("Content-Type", "application/json")
                .exchange().expectStatus().isOk()
                .expectBody(LookupResult.class).consumeWith(response -> {

            assertThat(response.getResponseBody().getType()).isEqualTo("LOOKUP");
            assertThat(response.getResponseBody().getMetric()).isEqualTo("cpu_active");
            assertThat(response.getResponseBody().getResults().size()).isEqualTo(3);

            response.getResponseBody().getResults().forEach(seriesData -> {
                seriesData.getTags().entrySet().stream().forEach(s -> {
                            assertTrue(
                                    // The order of the results are not deterministic
                                    (s.getKey().equals("os") && s.getValue().equals("linux")) ||
                                            (s.getKey().equals("host") && s.getValue().equals("h-1")) ||
                                            (s.getKey().equals("host") && s.getValue().equals("h-2"))
                            );
                        }
                );
            });
        });

        verify(metadataService).getTagKeys("t-1", "cpu_active");
        verify(metadataService).getTagValues("t-1", "cpu_active", "os");
        verify(metadataService).getTagValues("t-1", "cpu_active", "host");
        verify(metadataService).getMetricNameAndTags("cpu_active");
        verifyNoMoreInteractions(metadataService);
    }

    @Test
    public void testTagValues() {
        String tagValues = "{host=*}";
        String m = "cpu_active" + tagValues;

        when(metadataService.getTagKeys("t-1", m))
                .thenReturn(Mono.just(List.of("os", "host")));
        when(metadataService.getTagValues("t-1", m, "os"))
                .thenReturn(Mono.just(List.of("linux")));
        when(metadataService.getTagValues("t-1", m, "host"))
                .thenReturn(Mono.just(List.of("h-1", "h-2")));
        List<Map<String, String>> tags = new ArrayList<>();
        tags.add(Map.of("host", "*"));
        when(metadataService.getMetricNameAndTags(m))
                .thenReturn(new MetricNameAndMultiTags()
                        .setMetricName(m)
                        .setTags(tags));

        webTestClient.get().uri(
                uriBuilder -> uriBuilder.path("/api/search/lookup")
                        .queryParam("m", "cpu_active{tagValues}")
                        .queryParam("limit", 2).build(tagValues))
                .header("X-Tenant", "t-1")
                .header("Content-Type", "application/json")
                .exchange().expectStatus().isOk()
                .expectBody(LookupResult.class).consumeWith(response -> {

            assertThat(response.getResponseBody().getType()).isEqualTo("LOOKUP");
            assertThat(response.getResponseBody().getMetric()).isEqualTo(m);
            assertThat(response.getResponseBody().getLimit()).isEqualTo(2);
            assertThat(response.getResponseBody().getResults().size()).isEqualTo(2);

            response.getResponseBody().getResults().forEach(seriesData -> {
                seriesData.getTags().entrySet().stream().forEach(s -> {
                            assertTrue(
                                    (s.getKey().equals("host") && s.getValue().equals("h-1")) ||
                                            (s.getKey().equals("host") && s.getValue().equals("h-2"))
                            );
                        }
                );
            });
        });

        verify(metadataService).getTagKeys("t-1", m);
        verify(metadataService).getTagValues("t-1", m, "os");
        verify(metadataService).getTagValues("t-1", m, "host");
        verify(metadataService).getMetricNameAndTags(m);
        verifyNoMoreInteractions(metadataService);
    }

    @Test
    public void testTagValuesMixed() {
        String tagValues = "{host=*,os=linux,os=windows}";
        String m = "cpu_active" + tagValues;

        when(metadataService.getTagKeys("t-1", m))
                .thenReturn(Mono.just(List.of("os", "host")));
        when(metadataService.getTagValues("t-1", m, "os"))
                .thenReturn(Mono.just(List.of("linux", "windows")));
        when(metadataService.getTagValues("t-1", m, "host"))
                .thenReturn(Mono.just(List.of("h-1", "h-2")));
        List<Map<String, String>> tags = new ArrayList<>();
        tags.add(Map.of("host", "*"));
        tags.add(Map.of("os", "linux"));
        tags.add(Map.of("os", "windows"));
        when(metadataService.getMetricNameAndTags(m))
                .thenReturn(new MetricNameAndMultiTags()
                        .setMetricName(m)
                        .setTags(tags));

        webTestClient.get().uri(
                uriBuilder -> uriBuilder.path("/api/search/lookup")
                        .queryParam("m", "cpu_active{tagValues}")
                        .queryParam("limit", 4).build(tagValues))
                .header("X-Tenant", "t-1")
                .header("Content-Type", "application/json")
                .exchange().expectStatus().isOk()
                .expectBody(LookupResult.class).consumeWith(response -> {

            assertThat(response.getResponseBody().getType()).isEqualTo("LOOKUP");
            assertThat(response.getResponseBody().getMetric()).isEqualTo(m);
            assertThat(response.getResponseBody().getLimit()).isEqualTo(4);
            assertThat(response.getResponseBody().getResults().size()).isEqualTo(4);

            response.getResponseBody().getResults().forEach(seriesData -> {
                seriesData.getTags().entrySet().stream().forEach(s -> {
                            assertTrue(
                                    (s.getKey().equals("host") && s.getValue().equals("h-1")) ||
                                            (s.getKey().equals("host") && s.getValue().equals("h-2")) ||
                                            (s.getKey().equals("os") && s.getValue().equals("linux")) ||
                                            (s.getKey().equals("os") && s.getValue().equals("windows"))
                            );
                        }
                );
            });
        });

        verify(metadataService).getTagKeys("t-1", m);
        verify(metadataService).getTagValues("t-1", m, "os");
        verify(metadataService).getTagValues("t-1", m, "host");
        verify(metadataService).getMetricNameAndTags(m);
        verifyNoMoreInteractions(metadataService);
    }

    @Test
    public void testTagKeysWildcard() {
        String tagValues = "{*=linux}";
        String m = "cpu_active" + tagValues;

        when(metadataService.getTagKeys("t-1", m))
                .thenReturn(Mono.just(List.of("os", "host")));
        when(metadataService.getTagValues("t-1", m, "os"))
                .thenReturn(Mono.just(List.of("linux", "windows")));
        when(metadataService.getTagValues("t-1", m, "host"))
                .thenReturn(Mono.just(List.of("linux", "h-2")));
        List<Map<String, String>> tags = new ArrayList<>();
        tags.add(Map.of("*", "linux"));
        when(metadataService.getMetricNameAndTags(m))
                .thenReturn(new MetricNameAndMultiTags()
                        .setMetricName(m)
                        .setTags(tags));

        webTestClient.get().uri(
                uriBuilder -> uriBuilder.path("/api/search/lookup")
                        .queryParam("m", "cpu_active{tagValues}")
                        .queryParam("limit", 4).build(tagValues))
                .header("X-Tenant", "t-1")
                .header("Content-Type", "application/json")
                .exchange().expectStatus().isOk()
                .expectBody(LookupResult.class).consumeWith(response -> {

            assertThat(response.getResponseBody().getType()).isEqualTo("LOOKUP");
            assertThat(response.getResponseBody().getMetric()).isEqualTo(m);
            assertThat(response.getResponseBody().getLimit()).isEqualTo(4);
            assertThat(response.getResponseBody().getResults().size()).isEqualTo(2);

            response.getResponseBody().getResults().forEach(seriesData -> {
                seriesData.getTags().entrySet().stream().forEach(s -> {
                            assertTrue(
                                    (s.getKey().equals("host") && s.getValue().equals("linux")) ||
                                            (s.getKey().equals("os") && s.getValue().equals("linux"))
                            );
                        }
                );
            });
        });

        verify(metadataService).getTagKeys("t-1", m);
        verify(metadataService).getTagValues("t-1", m, "os");
        verify(metadataService).getTagValues("t-1", m, "host");
        verify(metadataService).getMetricNameAndTags(m);
        verifyNoMoreInteractions(metadataService);
    }
}
