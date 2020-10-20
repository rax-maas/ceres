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
import com.rackspace.ceres.app.model.SeriesSetCacheKey;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.cache.CaffeineCacheMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CacheConfig {

  private final MeterRegistry meterRegistry;
  private final AppProperties appProperties;

  @Autowired
  public CacheConfig(MeterRegistry meterRegistry,
                     AppProperties appProperties) {
    this.meterRegistry = meterRegistry;
    this.appProperties = appProperties;
  }

  @Bean
  public AsyncCache<SeriesSetCacheKey,Boolean/*exists*/> seriesSetExistenceCache() {
    final AsyncCache<SeriesSetCacheKey, Boolean> cache = Caffeine
        .newBuilder()
        .maximumSize(appProperties.getSeriesSetCacheSize())
        .recordStats()
        .buildAsync();

    // hook up to micrometer since we're not going through Spring Cache
    CaffeineCacheMetrics.monitor(meterRegistry, cache, "seriesSetExistence");

    return cache;
  }
}
