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
import com.rackspace.ceres.app.model.*;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.rackspace.ceres.app.utils.DateTimeUtils.nowEpochSeconds;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final String DELIM = "|";

  private final DownsampleProperties properties;
  private final HashService hashService;
  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetJob;
  private final AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache;

  private static final String GET_HASHES_TO_DOWNSAMPLE =
      "SELECT hash FROM downsampling_hashes WHERE partition = ? ALLOW FILTERING";

  private final Counter dbOperationErrorsCounter;
  private final AppProperties appProperties;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   ReactiveCassandraTemplate cassandraTemplate,
                                   RedisScript<String> redisGetJob,
                                   DownsampleProperties properties,
                                   HashService hashService,
                                   MeterRegistry meterRegistry,
                                   AppProperties appProperties,
                                   @Qualifier("downsample") AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache,
                                   ReactiveCqlTemplate cqlTemplate) {
    this.redisTemplate = redisTemplate;
    this.redisGetJob = redisGetJob;
    this.properties = properties;
    this.hashService = hashService;
    this.cqlTemplate = cqlTemplate;
    this.cassandraTemplate = cassandraTemplate;
    this.appProperties = appProperties;
    this.dbOperationErrorsCounter = meterRegistry.counter("ceres.db.operation.errors","type", "write");
    this.downsampleHashExistenceCache = downsampleHashExistenceCache;
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
    return redisTemplate.opsForValue().set(String.format("job|%d|%s", partition, group), "free");
  }

  public Mono<String> getTimeSlot(Integer partition, String group) {
    String key = String.format("pending|%d|%s", partition, group);
    long nowSeconds = nowEpochSeconds();
    long partitionWidth = Duration.parse(group).getSeconds();
    return this.redisTemplate.opsForSet().scan(key)
        .sort()
        .filter(timeslot -> isTimeslotDue(timeslot, partition, group, nowSeconds, partitionWidth))
        .next()
        // TODO remove this when the other things starts working!!
        .flatMap(t -> deleteTimeslot(partition, group, Long.parseLong(t)).then(Mono.just(t)));
  }

  private boolean isTimeslotDue(String timeslot, Integer partition, String group, long nowSeconds, long partitionWidth) {
    boolean isDue = Long.parseLong(timeslot) < nowSeconds - partitionWidth;
    if (isDue) {
      log.trace("is due: {} {} {} {}",
          partition, group, timeslot, Long.parseLong(timeslot) < nowSeconds - partitionWidth);
    }
    return isDue;
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }
    final HashCode hashCode = hashService.getHashCode(tenant, seriesSetHash);
    final String setHash = encodeSetHash(tenant, seriesSetHash);
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());
    return cacheDownsamplingHash(partition, setHash).then(saveTimeslots(partition, timestamp));
  }

  private Mono<?> saveTimeslots(int partition, Instant timestamp) {
    // TODO cache these also!!
    return Flux.fromIterable(DateTimeUtils.getPartitionWidths(properties.getGranularities())).flatMap(
        group -> {
          final Instant normalizedTimeSlot = timestamp.with(new TemporalNormalizer(Duration.parse(group)));
          final long timeslot = normalizedTimeSlot.getEpochSecond();
          return saveTimeslot(partition, group, timeslot);
        }
    ).then(Mono.empty());
  }

  private Mono<?> cacheDownsamplingHash(Integer partition, String setHash) {
    final CompletableFuture<Boolean> result = downsampleHashExistenceCache.get(
            new DownsampleSetCacheKey(partition, setHash),
            (key, executor) -> {
              // TODO: This cache doesn't seem to work. I got 500 different hashes to test with but it still keeps saving
              log.trace("saving downsampling hash {} {}", partition, setHash);
              return saveDownsampling(partition, setHash)
                      .flatMap(s -> Mono.just(true))
                      .toFuture();
            }
    );
    return Mono.fromFuture(result);
  }

  private Mono<?> saveDownsampling(Integer partition, String setHash) {
    log.trace("saveDownsampling_ {} {}", partition, setHash);
    return this.cassandraTemplate.insert(new Downsampling(partition, setHash))
            .name("saveDownsampling")
            .metrics()
            .retryWhen(appProperties.getRetryInsertDownsampled().build());
  }

  private Mono<?> saveTimeslot(Integer partition, String group, Long timeslot) {
    log.trace("saveTimeslot {} {} {}", partition, group, timeslot);
    String key = String.format("pending|%d|%s", partition, group);
    return redisTemplate.opsForSet().add(key, timeslot.toString());
  }

  public Mono<?> deleteTimeslot(Integer partition, String group, Long timeslot) {
    String key = String.format("pending|%d|%s", partition, group);
    return redisTemplate.opsForSet().remove(key, timeslot.toString())
        .flatMap(result -> {
          log.info("Delete timeslot result: {} {} {} {}", result, partition, group,
              Instant.ofEpochSecond(timeslot).atZone(ZoneId.systemDefault()).toLocalDateTime());
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
    return tenant + DELIM + setHash;
  }

  private static PendingDownsampleSet buildPending(Long timeslot, String setHash) {
    log.trace("build pending: {} {}", timeslot, setHash);
    final int splitValueAt = setHash.indexOf(DELIM);
    return new PendingDownsampleSet()
        .setTenant(setHash.substring(0, splitValueAt))
        .setSeriesSetHash(setHash.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(timeslot));
  }
}
