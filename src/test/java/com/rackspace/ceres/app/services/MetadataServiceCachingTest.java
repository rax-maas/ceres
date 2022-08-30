/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.ceres.app.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.CacheConfig;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.entities.TagsData;
import com.rackspace.ceres.app.model.Metric;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import com.rackspace.ceres.app.model.SuggestType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.query.Query;
import reactor.core.publisher.Mono;

/**
 * This unit test mocks out all datastore interactions to verify the different levels of caching
 * behavior with {@link MetadataService#storeMetadata(String, String, Metric)}.
 */
@SpringBootTest(classes = {
    CacheConfig.class,
    MetadataService.class,
    SimpleMeterRegistry.class
}, properties = {
    "ceres.series-set-cache-size=1",
    "ceres.elastic-search-index-name=metrics",
    "ceres.elasticsearch.host=localhost",
    "ceres.elasticsearch.port=9200"
})
@EnableConfigurationProperties({AppProperties.class, DownsampleProperties.class})
public class MetadataServiceCachingTest {

  @MockBean
  ReactiveCqlTemplate cqlTemplate;

  @MockBean
  ReactiveCassandraTemplate cassandraTemplate;

  @Autowired
  MetadataService metadataService;

  @MockBean
  ElasticSearchService elasticSearchService;

  @Autowired
  AsyncCache<SeriesSetCacheKey,Boolean/*exists*/> seriesSetExistenceCache;

  @AfterEach
  void tearDown() {
    seriesSetExistenceCache.synchronous().invalidateAll();
    reset(cassandraTemplate);
  }

  @Test
  void writeThruToCassandraAtFirst() {
    when(cassandraTemplate.insert(any(Object.class)))
        .thenReturn(Mono.just(new Object()));
    when(cqlTemplate.execute(any(String.class)))
            .thenReturn(Mono.just(Boolean.TRUE));
    when(cassandraTemplate.exists(any(Query.class), any(Class.class)))
            .thenReturn(Mono.just(false));
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());
    final String tenant = randomAlphanumeric(10);
    final String seriesSetHash = randomAlphanumeric(10);
    final String metricName = randomAlphanumeric(10);
    final String tagK = randomAlphanumeric(5);
    final String tagV = randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(tagK, tagV);
    Metric metric = new Metric()
            .setMetric(metricName)
            .setTags(tags)
                    .setTimestamp(Instant.now());

    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash, metric)
    ).block();

    verify(cassandraTemplate).insert(
        new SeriesSetHash()
            .setTenant(tenant)
            .setSeriesSetHash(seriesSetHash)
            .setMetricName(metricName)
            .setTags(tags)
    );
    verify(cassandraTemplate).insert(
        new MetricName()
            .setTenant(tenant)
            .setMetricName(metricName)
    );
    verify(cassandraTemplate).insert(
        new SeriesSet()
            .setTenant(tenant)
            .setMetricName(metricName)
            .setTagKey(tagK)
            .setTagValue(tagV)
            .setSeriesSetHash(seriesSetHash)
    );
    verify(cassandraTemplate).insert(
        new TagsData()
            .setTenant(tenant)
            .setType(SuggestType.TAGK)
            .setData(tagK)
    );
    verify(cassandraTemplate).insert(
        new TagsData()
            .setTenant(tenant)
            .setType(SuggestType.TAGV)
            .setData(tagV)
    );
    verify(cassandraTemplate).exists(any(Query.class), any(Class.class));
    verifyNoMoreInteractions(cqlTemplate, cassandraTemplate);
  }

  @Test
  void skipMetadataSaveForDuplicateMetric() {
    final String tenant = randomAlphanumeric(10);
    final String seriesSetHash = randomAlphanumeric(10);
    final String metricName = randomAlphanumeric(10);
    final String tagK = randomAlphanumeric(5);
    final String tagV = randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(tagK, tagV);

    when(cassandraTemplate.exists(any(Query.class), any(Class.class)))
            .thenReturn(Mono.just(true));
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash, new Metric().setMetric(metricName).setTags(tags))
    ).block();

    verify(cassandraTemplate).exists(any(Query.class), any(Class.class));
    verifyNoMoreInteractions(cqlTemplate, cassandraTemplate);
  }

  /**
   * The SpringBootTest at the top sets the cache limit to 1 and this unit test confirms
   * that entries are cached and evicted, as configured.
   */
  @Test
  void confirmMaxCacheConfig() {
    final String tenant = randomAlphanumeric(10);
    final String seriesSetHash1 = randomAlphanumeric(10);
    final String seriesSetHash2 = randomAlphanumeric(10);
    final String metricName = randomAlphanumeric(10);
    final String tagK = randomAlphanumeric(5);
    final String tagV = randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(tagK, tagV);
    // Given
    assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(0);
    when(cassandraTemplate.exists(any(Query.class), any(Class.class)))
            .thenReturn(Mono.just(true));
    when(elasticSearchService.saveMetricToES(anyString(), any(Metric.class)))
        .thenReturn(Mono.empty());

    // When
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash1, new Metric().setMetric(metricName).setTags(tags))
    ).block();

    // Then
    await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
        assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(1)
    );

    // store the same again, but it should hit cache and not cassandra
    // When
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash1, new Metric().setMetric(metricName).setTags(tags))
    ).block();

    // Then
    await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
        assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(1)
    );

    // When
    // store a different one to displace the one cache entry
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash2, new Metric().setMetric(metricName).setTags(tags))
    ).block();

    // Then
    await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
        assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(1)
    );

    // and back to the first to confirm another cache miss
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash1, new Metric().setMetric(metricName).setTags(tags))
    ).block();

    verify(cassandraTemplate, times(3))
        .exists(any(Query.class), any(Class.class));
    verifyNoMoreInteractions(cqlTemplate, cassandraTemplate);
  }
}
