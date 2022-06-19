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
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.DownsampleSetCacheKey;
import com.rackspace.ceres.app.model.Downsampling;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.model.TimeslotCacheKey;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.rackspace.ceres.app.utils.DateTimeUtils.nowEpochSeconds;
import static com.rackspace.ceres.app.utils.DateTimeUtils.epochToLocalDateTime;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private final DownsampleProperties properties;
  private final SeriesSetService hashService;
  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetJob;
  private final AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache;
  private final AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache;

  private static final String GET_HASHES_TO_DOWNSAMPLE = "SELECT hash FROM downsampling_hashes WHERE partition = ?";

  private final AppProperties appProperties;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   ReactiveCassandraTemplate cassandraTemplate,
                                   RedisScript<String> redisGetJob,
                                   DownsampleProperties properties,
                                   SeriesSetService hashService,
                                   AppProperties appProperties,
                                   AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache,
                                   AsyncCache<TimeslotCacheKey, Boolean> timeslotExistenceCache,
                                   ReactiveCqlTemplate cqlTemplate) {
    this.redisTemplate = redisTemplate;
    this.redisGetJob = redisGetJob;
    this.properties = properties;
    this.hashService = hashService;
    this.cqlTemplate = cqlTemplate;
    this.cassandraTemplate = cassandraTemplate;
    this.appProperties = appProperties;
    this.downsampleHashExistenceCache = downsampleHashExistenceCache;
    this.timeslotExistenceCache = timeslotExistenceCache;
  }

  public Flux<String> claimJob(Integer partition, String group) {
    log.trace("claim job partition, group {} {}", partition, group);
    String hostName = System.getenv("HOSTNAME");
    long now = Instant.now().getEpochSecond();
    long maxJobDuration = properties.getMaxDownsampleJobDuration().getSeconds();
    return redisTemplate.execute(
        this.redisGetJob,
        List.of(),
        List.of(partition.toString(),
            group,
            hostName == null ? "localhost" : hostName,
            Long.toString(now),
            Long.toString(maxJobDuration))
    );
  }

  public Mono<?> freeJob(int partition, String group) {
    log.trace("free job partition, group {} {}", partition, group);
    return redisTemplate.opsForValue().set(encodeJobKey(partition, group), "free");
  }

  public Mono<String> getTimeSlot(Integer partition, String group) {
    long partitionWidth = Duration.parse(group).getSeconds();
    String key = encodeTimeslotKey(partition, group);
    return this.redisTemplate.opsForSet().scan(key)
        .sort() // Make sure oldest timeslot is first
        .filter(t -> isTimeslotDue(partition, group, t, partitionWidth))
        .next()
        .flatMap(timeslot -> {
              this.timeslotExistenceCache.synchronous()
                  .invalidate(new TimeslotCacheKey(partition, group, Long.parseLong(timeslot)));
              return this.redisTemplate.opsForSet().remove(key, timeslot)
                  .then(this.redisTemplate.opsForSet().add(key, encodeTimeslotInProgress(timeslot)))
                  .then(Mono.just(timeslot));
            }
        );
  }

  private boolean isTimeslotDue(int partition, String group, String timeslot, long partitionWidth) {
    if (timeslot.matches(".*in-progress")) {
      long ts = Long.parseLong(timeslot.split("\\|")[0]);
      if (ts < (nowEpochSeconds() - partitionWidth * 3)) {
        // This timeslot is hanging in state in-progress
        log.warn("Setting in-progress timeslot back to pending: {} {} {}", partition, group, epochToLocalDateTime(ts));
        String key = encodeTimeslotKey(partition, group);
        this.redisTemplate.opsForSet().remove(key, timeslot)
            .and(this.redisTemplate.opsForSet().add(key, Long.toString(ts))).subscribe();
      }
      return false;
    } else {
      return Long.parseLong(timeslot) < nowEpochSeconds() - partitionWidth;
    }
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      log.error("Partitions are not configured");
      return Mono.empty();
    }
    final HashCode hashCode = hashService.getHashCode(tenant, seriesSetHash);
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());
    final String setHash = encodeSetHash(tenant, seriesSetHash);
    return saveDownsampling(partition, setHash).then(saveTimeslots(partition, timestamp));
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

  private Mono<?> saveTimeslots(int partition, Instant timestamp) {
    return Flux.fromIterable(DateTimeUtils.getPartitionWidths(properties.getGranularities())).flatMap(
        group -> {
          final Instant normalizedTimeslot = DateTimeUtils.normalizedTimeslot(timestamp, group);
          return saveTimeslot(partition, group, normalizedTimeslot.getEpochSecond());
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

  public Mono<?> deleteTimeslot(Integer partition, String group, Long timeslot) {
    return redisTemplate.opsForSet()
        .remove(encodeTimeslotKey(partition, group), encodeTimeslotInProgress(timeslot))
        .flatMap(result -> {
              log.info("Deleted timeslot: {} {} {} {}", result, partition, group, epochToLocalDateTime(timeslot));
              return Mono.just(result);
            }
        );
  }

  public Flux<PendingDownsampleSet> getDownsampleSets(Long timeslot, int partition) {
    log.trace("getDownsampleSets {} {}", timeslot, partition);
    return this.cqlTemplate.queryForFlux(GET_HASHES_TO_DOWNSAMPLE, String.class, partition)
        .map(setHash -> buildPending(timeslot, setHash));
  }

  private static String encodeSetHash(String tenant, String setHash) {
    return String.format("%s|%s", tenant, setHash);
  }

  private static String encodeTimeslotKey(int partition, String group) {
    return String.format("pending|%d|%s", partition, group);
  }

  private static String encodeTimeslotInProgress(String timeslot) {
    return String.format("%s|in-progress", timeslot);
  }

  private static String encodeTimeslotInProgress(long timeslot) {
    return String.format("%d|in-progress", timeslot);
  }

  private static String encodeJobKey(int partition, String group) {
    return String.format("job|%d|%s", partition, group);
  }

  private static PendingDownsampleSet buildPending(Long timeslot, String setHash) {
    log.trace("build pending: {} {}", timeslot, setHash);
    final int splitValueAt = setHash.indexOf("|");
    return new PendingDownsampleSet()
        .setTenant(setHash.substring(0, splitValueAt))
        .setSeriesSetHash(setHash.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(timeslot));
  }
}
