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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils.randomAlphanumeric;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.CacheConfig;
import com.rackspace.ceres.app.entities.MetricName;
import com.rackspace.ceres.app.entities.SeriesSet;
import com.rackspace.ceres.app.entities.SeriesSetHash;
import com.rackspace.ceres.app.entities.TagsData;
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import com.rackspace.ceres.app.model.SuggestType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;

/**
 * This unit test mocks out all datastore interactions to verify the different levels of caching
 * behavior with {@link MetadataService#storeMetadata(String, String, String, Map)}.
 */
@SpringBootTest(classes = {
    CacheConfig.class,
    MetadataService.class,
    SimpleMeterRegistry.class
}, properties = {
    "ceres.series-set-cache-size=1"
})
@EnableConfigurationProperties(AppProperties.class)
public class MetadataServiceCachingTest {

  @MockBean
  ReactiveCqlTemplate cqlTemplate;

  @MockBean
  ReactiveCassandraTemplate cassandraTemplate;

  @MockBean
  ReactiveStringRedisTemplate redisTemplate;

  @Mock
  ReactiveValueOperations<String, String> opsForValue;

  @Autowired
  MetadataService metadataService;

  @Autowired
  AsyncCache<SeriesSetCacheKey,Boolean/*exists*/> seriesSetExistenceCache;

  @AfterEach
  void tearDown() {
    seriesSetExistenceCache.synchronous().invalidateAll();
  }

  @Test
  void writeThruToCassandraAtFirst() {
    when(cassandraTemplate.insert(any(Object.class)))
        .thenReturn(Mono.empty());

    when(redisTemplate.opsForValue())
        .thenReturn(opsForValue);
    when(opsForValue.setIfAbsent(any(), any()))
        .thenReturn(Mono.just(true));

    final String tenant = randomAlphanumeric(10);
    final String seriesSetHash = randomAlphanumeric(10);
    final String metricName = randomAlphanumeric(10);
    final String tagK = randomAlphanumeric(5);
    final String tagV = randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(tagK, tagV);

    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash, metricName, tags)
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

    verify(redisTemplate).opsForValue();
    verify(opsForValue)
        .setIfAbsent(String.format("seriesSetHashes|%s|%s", tenant, seriesSetHash), "");

    verifyNoMoreInteractions(cqlTemplate, cassandraTemplate, redisTemplate, opsForValue);
  }

  @Test
  void stopAtRedisWhenPresentThere() {
    when(redisTemplate.opsForValue())
        .thenReturn(opsForValue);
    when(opsForValue.setIfAbsent(any(), any()))
        // this time redis says the key was present
        .thenReturn(Mono.just(false));

    final String tenant = randomAlphanumeric(10);
    final String seriesSetHash = randomAlphanumeric(10);
    final String metricName = randomAlphanumeric(10);
    final String tagK = randomAlphanumeric(5);
    final String tagV = randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(tagK, tagV);

    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash, metricName, tags)
    ).block();

    verify(redisTemplate).opsForValue();
    verify(opsForValue)
        .setIfAbsent(String.format("seriesSetHashes|%s|%s", tenant, seriesSetHash), "");

    // store the same again, but it should hit cache and not redis
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash, metricName, tags)
    ).block();

    verifyNoMoreInteractions(cqlTemplate, cassandraTemplate, redisTemplate, opsForValue);
  }

  /**
   * The SpringBootTest at the top sets the cache limit to 1 and this unit test confirms
   * that entries are cached and evicted, as configured.
   */
  @Test
  void confirmMaxCacheConfig() {
    when(redisTemplate.opsForValue())
        .thenReturn(opsForValue);
    when(opsForValue.setIfAbsent(any(), any()))
        // this time redis says the key was present
        .thenReturn(Mono.just(false));

    final String tenant = randomAlphanumeric(10);
    final String seriesSetHash1 = randomAlphanumeric(10);
    final String seriesSetHash2 = randomAlphanumeric(10);
    final String metricName = randomAlphanumeric(10);
    final String tagK = randomAlphanumeric(5);
    final String tagV = randomAlphanumeric(5);
    final Map<String, String> tags = Map.of(tagK, tagV);

    assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(0);

    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash1, metricName, tags)
    ).block();

    await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
        assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(1)
    );

    // store the same again, but it should hit cache and not redis
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash1, metricName, tags)
    ).block();

    await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
        assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(1)
    );

    // store a different one to displace the one cache entry
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash2, metricName, tags)
    ).block();

    await().atMost(Duration.ofSeconds(1)).untilAsserted(() ->
        assertThat(seriesSetExistenceCache.synchronous().estimatedSize()).isEqualTo(1)
    );

    // and back to the first to confirm another cache miss
    Mono.from(
        metadataService.storeMetadata(tenant, seriesSetHash1, metricName, tags)
    ).block();

    verify(opsForValue, times(2))
        .setIfAbsent(String.format("seriesSetHashes|%s|%s", tenant, seriesSetHash1), "");
    verify(opsForValue)
        .setIfAbsent(String.format("seriesSetHashes|%s|%s", tenant, seriesSetHash2), "");

    verify(redisTemplate, times(3)).opsForValue();

    verifyNoMoreInteractions(cqlTemplate, cassandraTemplate, redisTemplate, opsForValue);
  }
}
