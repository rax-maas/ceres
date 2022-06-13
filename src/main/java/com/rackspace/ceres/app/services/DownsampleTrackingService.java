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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
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
  private final String hostname;
  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveCassandraTemplate cassandraTemplate;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetJob;
  private final AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache;

  private static final String GET_TIMESLOT_QUERY = "SELECT timeslot FROM pending_timeslots WHERE partition = ? AND group = ? AND timeslot <= ? LIMIT 1";
  private static final String GET_HASHES_TO_DOWNSAMPLE = "SELECT hash FROM downsampling_hashes WHERE timeslot = ? AND group = ? AND partition = ?  AND completed = false LIMIT ? ALLOW FILTERING";
  private static final String REMOVE_DOWNSAMPLING_TIMESLOT_PARTITION = "DELETE FROM downsampling_hashes WHERE timeslot = ? AND group = ? AND partition = ?";

  private final Counter dbOperationErrorsCounter;
  private final AppProperties appProperties;
  private final MeterRegistry meterRegistry;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   ReactiveCassandraTemplate cassandraTemplate,
                                   RedisScript<String> redisGetJob,
                                   DownsampleProperties properties,
                                   HashService hashService,
                                   MeterRegistry meterRegistry,
                                   AppProperties appProperties,
                                   @Qualifier("downsample") AsyncCache<DownsampleSetCacheKey, Boolean> downsampleHashExistenceCache,
                                   ReactiveCqlTemplate cqlTemplate) throws UnknownHostException {
    this.redisTemplate = redisTemplate;
    this.redisGetJob = redisGetJob;
    this.properties = properties;
    this.hostname = InetAddress.getLocalHost().getHostName();
    this.hashService = hashService;
    this.cqlTemplate = cqlTemplate;
    this.cassandraTemplate = cassandraTemplate;
    this.appProperties = appProperties;
    dbOperationErrorsCounter = meterRegistry.counter("ceres.db.operation.errors",
            "type", "write");
    this.downsampleHashExistenceCache = downsampleHashExistenceCache;
    this.meterRegistry = meterRegistry;
  }

  public Flux<String> checkPartitionJob(Integer partition, String group) {
    String hostName = System.getenv("HOSTNAME");
    Long now = Instant.now().getEpochSecond();
    Long maxJobDuration = properties.getMaxDownsampleJobDuration().getSeconds();
    return redisTemplate.execute(
            this.redisGetJob,
            List.of(),
            List.of(partition.toString(),
                    group,
                    hostName == null ? "localhost" : hostName,
                    now.toString(),
                    maxJobDuration.toString())
            );
  }

  public Mono<Long> getTimeSlot(Integer partition, String group) {
    long nowSeconds = nowEpochSeconds();
    long partitionWidth = Duration.parse(group).getSeconds();
    return this.cqlTemplate.queryForObject(GET_TIMESLOT_QUERY,
            Long.class, partition, group, nowSeconds-partitionWidth).doOnError(error -> {
              log.error("getTimeSlot failed", error);
              dbOperationErrorsCounter.increment();
    });
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }
    final HashCode hashCode = hashService.getHashCode(tenant, seriesSetHash);
    final String pendingValue = encodingPendingValue(tenant, seriesSetHash);
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());

    return Flux.fromIterable(DateTimeUtils.getPartitionWidths(properties.getGranularities())).flatMap(
        width -> {
          final Instant normalizedTimeSlot = timestamp.with(new TemporalNormalizer(Duration.parse(width)));
          final long timeslot = normalizedTimeSlot.getEpochSecond();
          return cacheDownsamplingHash(partition, width, timeslot, pendingValue);
        }
    );
  }

  private Mono<?> cacheDownsamplingHash(Integer partition, String width, long timeslot, String hash) {
    final CompletableFuture<Boolean> result = downsampleHashExistenceCache.get(
            new DownsampleSetCacheKey(partition, width, timeslot, hash),
            (key, executor) -> {
              log.trace("saving downsampling hash {} {} {} {}", partition, width, timeslot, hash);
              return saveDownsampling(partition, width, timeslot, hash)
                      .then(savePending(partition, width, timeslot))
                      .flatMap(s -> Mono.just(true))
                      .toFuture();
            }
    );
    return Mono.fromFuture(result);
  }

  private Mono<?> saveDownsampling(Integer partition, String width, long timeslot, String hash) {
    log.trace("saveDownsampling_ {} {} {} {}", partition, width, timeslot, hash);
    return this.cassandraTemplate.insert(new Downsampling(timeslot, width, partition, hash, false))
            .name("saveDownsampling")
            .metrics()
            .retryWhen(appProperties.getRetryInsertDownsampled().build());
  }

  private Mono<?> savePending(Integer partition, String width, long timeslot) {
    log.trace("savePending {} {} {}", partition, width, timeslot);
    Pending pending = new Pending(partition, width, timeslot);
    return this.cassandraTemplate.insert(pending)
            .name("savePending")
            .metrics()
            .retryWhen(appProperties.getRetryInsertDownsampled().build());
  }

  public Flux<PendingDownsampleSet> getDownsampleSets(Long timeslot, int partition, String group) {
    log.trace("getDownsampleSets {} {} {}", timeslot, partition, group);
    return this
            .cqlTemplate
            .queryForFlux(GET_HASHES_TO_DOWNSAMPLE, String.class, timeslot, group, partition, properties.getSetHashesProcessLimit())
            .map(hash -> buildPending(timeslot, hash));
  }

  public Mono<?> initJob(int partition, String group) {
    return redisTemplate
            .opsForValue()
            .setIfAbsent("job|" + partition + "|" + group, "free")
            .flatMap( updated -> {
              if(updated) {
                log.info("InitJob partition, group {} {}", partition, group);
                meterRegistry.counter("initJob", "partition", String.valueOf(partition), "group", group).increment();
              }
              return Mono.just(updated);
            });
  }

  public Mono<?> resetJob(int partition, String group) {
    log.trace("resetJob partition, group {} {}", partition, group);
    meterRegistry.counter("resetJob", "partition", String.valueOf(partition), "group", group).increment();
    return redisTemplate.opsForValue().set("job|" + partition + "|" + group, "free").doOnError( e -> e.printStackTrace());
  }

  public Mono<?> complete(PendingDownsampleSet entry, Integer partition, String group) {
    final Long timeslot = entry.getTimeSlot().getEpochSecond();
    final String value = encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash());
    return addToCompletedHashes(partition, group, timeslot, value)
            .and(removeEntryFromCache(partition,group, timeslot, value));
  }

  private Mono<?> addToCompletedHashes(Integer partition, String group, Long timeslot, String value) {
    return this.cassandraTemplate.insert(new Downsampling(timeslot, group, partition, value, true));
  }

  public Mono<?> deleteTimeslot(Integer partition, String group, Long timeslot) {
    log.debug("deleting partition, group, timeslot {} {} {}", partition, group, timeslot);
    return
            this.cassandraTemplate.delete(new Pending(partition, group, timeslot))
                    .and(this.cqlTemplate.execute(REMOVE_DOWNSAMPLING_TIMESLOT_PARTITION,timeslot, group, partition))
                    .doOnError((throwable) -> {
                      log.error("Exception in deleteTimeslot", throwable);
                    }) ;
  }

  public Mono<Boolean> removeEntryFromCache(Integer partition, String width, Long timeslot, String hash) {
    downsampleHashExistenceCache.synchronous().invalidate(new DownsampleSetCacheKey(partition, width, timeslot, hash));
    return Mono.just(true);
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant + DELIM + seriesSet;
  }

  private static PendingDownsampleSet buildPending(Long timeslot, String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);
    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(timeslot));
  }
}
