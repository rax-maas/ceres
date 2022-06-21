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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.google.common.hash.HashCode;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.DownsampleSetCacheKey;
import com.rackspace.ceres.app.model.Downsampling;
import com.rackspace.ceres.app.model.TimeslotCacheKey;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
@Profile("ingest")
public class IngestTrackingService {

  private final DownsampleProperties properties;
  private final AppProperties appProperties;
  private final SeriesSetService hashService;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache;
  private final AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache;

  @Autowired
  public IngestTrackingService(ReactiveStringRedisTemplate redisTemplate,
                               ReactiveCassandraTemplate cassandraTemplate,
                               DownsampleProperties properties,
                               SeriesSetService hashService,
                               AppProperties appProperties,
                               AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache,
                               AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    this.hashService = hashService;
    this.cassandraTemplate = cassandraTemplate;
    this.appProperties = appProperties;
    this.downsampleHashExistenceCache = downsampleHashExistenceCache;
    this.timeslotExistenceCache = timeslotExistenceCache;
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      log.error("Partitions are not configured");
      return Mono.empty();
    }
    final HashCode hashCode = hashService.getHashCode(tenant, seriesSetHash);
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());
    final String setHash = encodeSetHash(tenant, seriesSetHash);
    return saveDownsampling(partition, setHash).then(saveTimeslots(partition, timestamp, setHash));
  }

  private Mono<?> saveDownsampling(Integer partition, String setHash) {
    final CompletableFuture<Boolean> result = downsampleHashExistenceCache.get(
        new DownsampleSetCacheKey(partition, setHash),
        (key, executor) ->
            this.cassandraTemplate.insert(new Downsampling(partition, setHash))
                .name("saveDownsampling")
                .metrics()
                .retryWhen(appProperties.getRetryInsertDownsampled().build())
                .flatMap(s -> Mono.just(true))
                .toFuture()
    );
    return Mono.fromFuture(result);
  }

  private Mono<?> saveTimeslots(int partition, Instant timestamp, String setHash) {
    return Flux.fromIterable(DateTimeUtils.getPartitionWidths(properties.getGranularities())).flatMap(
        group -> {
          final Long normalizedTimeSlot = DateTimeUtils.normalizedTimeslot(timestamp, group);
          long partitionWidth = Duration.parse(group).getSeconds();
          if (normalizedTimeSlot + partitionWidth < DateTimeUtils.nowEpochSeconds()) {
            // Delayed timeslot, downsampling happened in the past
            return redisTemplate.opsForSet().add(
                encodeDelayedTimeslotKey(partition, group), String.format("%d|%s", normalizedTimeSlot, setHash));
          } else {
            // Downsampling in the future
            return saveTimeslot(partition, group, normalizedTimeSlot);
          }
        }
    ).then(Mono.empty());
  }

  private Mono<?> saveTimeslot(Integer partition, String group, Long timeslot) {
    final CompletableFuture<Boolean> result = timeslotExistenceCache.get(
        new TimeslotCacheKey(partition, group, timeslot),
        (key, executor) -> {
          log.trace("Saving timeslot {} {} {}", partition, group, timeslot);
          return redisTemplate.opsForSet().add(encodeTimeslotKey(partition, group), timeslot.toString())
              .flatMap(s -> Mono.just(true))
              .toFuture();
        }
    );
    return Mono.fromFuture(result);
  }

  private static String encodeSetHash(String tenant, String setHash) {
    return String.format("%s|%s", tenant, setHash);
  }

  private static String encodeTimeslotKey(int partition, String group) {
    return String.format("pending|%d|%s", partition, group);
  }

  private static String encodeDelayedTimeslotKey(int partition, String group) {
    return String.format("delayed|%d|%s", partition, group);
  }
}
