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

package com.rackspace.ceres.app.config;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.rackspace.ceres.app.model.*;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class CacheConfig {

  private final MeterRegistry meterRegistry;
  private final AppProperties appProperties;
  private final DownsampleProperties downsampleProperties;

  @Autowired
  public CacheConfig(MeterRegistry meterRegistry,
                     AppProperties appProperties,
                     DownsampleProperties downsampleProperties) {
    this.meterRegistry = meterRegistry;
    this.appProperties = appProperties;
    this.downsampleProperties = downsampleProperties;
  }

  @Bean
  public AsyncCache<SeriesSetCacheKey, Boolean/*exists*/> seriesSetExistenceCache() {
    final AsyncCache<SeriesSetCacheKey, Boolean> cache = Caffeine
        .newBuilder()
        .maximumSize(appProperties.getSeriesSetCacheSize())
        .recordStats()
        .buildAsync();
    CaffeineCacheMetrics.monitor(meterRegistry, cache, "seriesSetExistence");
    return cache;
  }

  @Bean
  public AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache() {
    final AsyncCache<DownsampleSetCacheKey, Boolean> cache = Caffeine
        .newBuilder()
        .maximumSize(downsampleProperties.getDownsampleHashCacheSize())
        .expireAfterWrite(appProperties.getDownsamplingHashesCacheTtl())
        .recordStats()
        .buildAsync();
    CaffeineCacheMetrics.monitor(meterRegistry, cache, "downsampleHashExistenceCache");
    return cache;
  }

  @Bean
  public AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache() {
    final AsyncCache<TimeslotCacheKey, Boolean> cache = Caffeine
        .newBuilder()
        .maximumSize(10000)
        .expireAfterWrite(appProperties.getDownsamplingTimeslotCacheTtl())
        .recordStats()
        .buildAsync();
    CaffeineCacheMetrics.monitor(meterRegistry, cache, "timeslotExistenceCache");
    return cache;
  }

  @Bean
  public AsyncCache<DownsampleSetCacheKey, Boolean> delayedDownsampleHashExistenceCache() {
    final AsyncCache<DownsampleSetCacheKey, Boolean> cache = Caffeine
        .newBuilder()
        .maximumSize(400000)
        .expireAfterWrite(appProperties.getDelayedHashesCacheTtl())
        .recordStats()
        .buildAsync();
    CaffeineCacheMetrics.monitor(meterRegistry, cache, "delayedDownsampleHashExistenceCache");
    return cache;
  }

  @Bean
  public AsyncCache<DelayedTimeslotCacheKey, Boolean> delayedTimeslotExistenceCache() {
    final AsyncCache<DelayedTimeslotCacheKey, Boolean> cache = Caffeine
        .newBuilder()
        .maximumSize(10000)
        // Expiration to allow for repeated updates of the same timeslot
        .expireAfterWrite(appProperties.getDelayedTimeslotCacheTtl())
        .recordStats()
        .buildAsync();
    CaffeineCacheMetrics.monitor(meterRegistry, cache, "delayedTimeslotExistenceCache");
    return cache;
  }
}
