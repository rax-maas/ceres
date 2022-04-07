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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final Charset HASHING_CHARSET = StandardCharsets.UTF_8;
  private static final String DELIM = "|";
  private static final String PREFIX_INGESTING = "ingesting";
  private static final String PREFIX_PENDING = "pending";
  private static final String PREFIX_DOWNSAMPLE = "downsample";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<Boolean> redisScript;
  private final DownsampleProperties properties;
  private final TemporalNormalizer timeSlotNormalizer;
  private final HashFunction hashFunction;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   RedisScript<Boolean> redisScript,
                                   DownsampleProperties properties) {
    this.redisTemplate = redisTemplate;
    this.redisScript = redisScript;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    timeSlotNormalizer = new TemporalNormalizer(properties.getTimeSlotWidth());
    hashFunction = Hashing.murmur3_32();
  }

  public Flux<Boolean> checkPartitionJobs(Integer jobKey, String now, String newTime) {
    return redisTemplate.execute(redisScript, List.of(jobKey.toString()), List.of(now, newTime));
  }

  public Mono<String> getJobValue(Integer jobKey) {
    return redisTemplate.opsForValue().get(jobKey.toString());
  }

  public Mono<Boolean> setJobValue(Integer jobKey, String isoTime) {
    return redisTemplate.opsForValue().set(jobKey.toString(), isoTime);
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }

    log.info("track: {}...", timestamp);

    final Instant normalizedTimeSlot = timestamp.with(timeSlotNormalizer);
    final String ingestingKey = encodeKey(PREFIX_INGESTING, normalizedTimeSlot);
    final String pendingValue = encodingPendingValue(tenant, seriesSetHash);
    final String timeslot = Long.toString(normalizedTimeSlot.getEpochSecond());

    return redisTemplate.opsForValue()
            .set(ingestingKey, "", properties.getLastTouchDelay())
            .and(redisTemplate.opsForSet().add(PREFIX_PENDING, timeslot))
            .and(redisTemplate.opsForSet().add(PREFIX_DOWNSAMPLE, timeslot))
            .and(redisTemplate.opsForSet().add(timeslot, pendingValue));
  }

  public Flux<PendingDownsampleSet> retrieveTimeSlots() {
    return redisTemplate.opsForSet()
            .scan(PREFIX_PENDING)
            .flatMap(timeslot ->
                    redisTemplate.hasKey(PREFIX_INGESTING + DELIM + timeslot)
                            .filter(stillIngesting -> !stillIngesting) // Filter out if still ingesting
                            .flatMapMany(ready -> allocateTimeSlot(timeslot).thenMany(getDownsampleSets(timeslot)))
            );
  }

  public Mono<?> allocateTimeSlot(String timeslot) {
    log.info("allocateTimeSlot: {}", timeslot);
    return redisTemplate.opsForSet().remove(PREFIX_PENDING, timeslot);
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot) {
    log.info("getPendingKeys: {}...", timeslot);
    return redisTemplate.opsForSet()
            .scan(timeslot)
            .map(pendingValue -> buildPending(timeslot, pendingValue));
  }

  public Mono<?> complete(PendingDownsampleSet entry) {
    log.info("complete: {} {} {}", entry.getTenant(), entry.getTimeSlot(), entry.getSeriesSetHash());
    String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    return redisTemplate.opsForSet()
            .remove(timeslot, encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash()))
            .and(redisTemplate.opsForSet().remove(PREFIX_DOWNSAMPLE, timeslot));
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant + DELIM + seriesSet;
  }

  private static String encodeKey(String prefix, Instant timeSlot) {
    return prefix + DELIM + timeSlot.getEpochSecond();
  }

  private static PendingDownsampleSet buildPending(String timeslot, String pendingValue) {
    log.info("buildPending: {} {}...", timeslot, pendingValue);

    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(Long.parseLong(timeslot)));
  }
}
