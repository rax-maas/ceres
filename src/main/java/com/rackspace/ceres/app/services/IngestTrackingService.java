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
import com.rackspace.ceres.app.entities.Downsampling;
import com.rackspace.ceres.app.model.DelayedTimeslotCacheKey;
import com.rackspace.ceres.app.model.DownsampleSetCacheKey;
import com.rackspace.ceres.app.model.TimeslotCacheKey;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class IngestTrackingService {

  private final DownsampleProperties properties;
  private final AppProperties appProperties;
  private final SeriesSetService hashService;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache;
  private final AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache;
  private final AsyncCache<DownsampleSetCacheKey, Boolean> delayedDownsampleHashExistenceCache;
  private final AsyncCache<DelayedTimeslotCacheKey, Boolean> delayedTimeslotExistenceCache;

  @Autowired
  public IngestTrackingService(ReactiveStringRedisTemplate redisTemplate,
                               ReactiveCassandraTemplate cassandraTemplate,
                               DownsampleProperties properties,
                               SeriesSetService hashService,
                               AppProperties appProperties,
                               AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache,
                               AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache,
                               AsyncCache<DownsampleSetCacheKey, Boolean> delayedDownsampleHashExistenceCache,
                               AsyncCache<DelayedTimeslotCacheKey, Boolean> delayedTimeslotExistenceCache) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    this.hashService = hashService;
    this.cassandraTemplate = cassandraTemplate;
    this.appProperties = appProperties;
    this.downsampleHashExistenceCache = downsampleHashExistenceCache;
    this.timeslotExistenceCache = timeslotExistenceCache;
    this.delayedDownsampleHashExistenceCache = delayedDownsampleHashExistenceCache;
    this.delayedTimeslotExistenceCache = delayedTimeslotExistenceCache;
  }

  @PostConstruct
  public void printConfigurations() {
    log.info("Start ingest tracking");
    log.info("downsampling-hashes-ttl: {}", appProperties.getDownsamplingHashesTtl().getSeconds());
    log.info("delayed-hashes-ttl: {}", appProperties.getDelayedHashesTtl().getSeconds());
    log.info("delayed-hashes-cache-ttl: {}", appProperties.getDelayedHashesCacheTtl().getSeconds());
    log.info("delayed-timeslot-cache-ttl: {}", appProperties.getDelayedTimeslotCacheTtl().getSeconds());
    log.info("downsample-delay-factor: {}", appProperties.getDownsampleDelayFactor());
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      log.error("Partitions are not configured");
      return Mono.empty();
    }
    final HashCode hashCode = hashService.getHashCode(tenant, seriesSetHash);
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());
    final String setHash = encodeSetHash(tenant, seriesSetHash);
    return saveDownsampling(partition, setHash).then(
        Flux.fromIterable(DateTimeUtils.getPartitionWidths(properties.getGranularities())).flatMap(
            group -> {
              final Long timeslot = DateTimeUtils.normalizedTimeslot(timestamp, group);
              long partitionWidth = Duration.parse(group).getSeconds();
              if ((timeslot + partitionWidth * appProperties.getDownsampleDelayFactor()) < DateTimeUtils.nowEpochSeconds()) {
                // Delayed timeslot, downsampling happened in the past
                return saveDelayedDownsampling(partition, setHash).and(saveDelayedTimeslot(partition, group, timeslot));
              } else {
                // Downsampling in the future
                return saveTimeslot(partition, group, timeslot);
              }
            }
        ).then(Mono.empty())
    );
  }

  private Mono<?> saveDelayedDownsampling(Integer partition, String setHash) {
    final CompletableFuture<Boolean> result = delayedDownsampleHashExistenceCache.get(
        new DownsampleSetCacheKey(partition, setHash),
        (key, executor) ->
            redisTemplate.opsForSet().add(encodeDelayedHashesKey(partition), setHash)
                .name("saveDelayedDownsampling")
                .metrics()
                .flatMap(s -> Mono.just(true))
                .toFuture()
    );
    return Mono.fromFuture(result);
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

  private Mono<?> saveTimeslot(Integer partition, String group, Long timeslot) {
    final CompletableFuture<Boolean> result = timeslotExistenceCache.get(
        new TimeslotCacheKey(partition, group, timeslot),
        (key, executor) ->
            redisTemplate.opsForSet().add(encodeTimeslotKey(partition, group), timeslot.toString())
                .flatMap(s -> Mono.just(true))
                .toFuture()
    );
    return Mono.fromFuture(result);
  }

  private Mono<?> saveDelayedTimeslot(Integer partition, String group, Long timeslot) {
    final CompletableFuture<Boolean> result = delayedTimeslotExistenceCache.get(
        new DelayedTimeslotCacheKey(partition, group, timeslot),
        (key, executor) ->
            redisTemplate.opsForSet()
                .add(encodeDelayedTimeslotKey(partition, group), timeslot.toString())
                .flatMap(s -> Mono.just(true))
                .toFuture()
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

  private static String encodeDelayedHashesKey(int partition) {
    return String.format("delayed-hashes|%d", partition);
  }
}
