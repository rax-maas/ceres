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
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final Charset HASHING_CHARSET = StandardCharsets.UTF_8;
  private static final String DELIM = "|";
  private static final String PREFIX_PENDING = "pending";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<List> redisGetTimeSlots;
  private final DownsampleProperties properties;
  private final TemporalNormalizer timeSlotNormalizer;
  private final HashFunction hashFunction;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   RedisScript<List> redisGetTimeSlots,
                                   DownsampleProperties properties) {
    this.redisTemplate = redisTemplate;
    this.redisGetTimeSlots = redisGetTimeSlots;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    timeSlotNormalizer = new TemporalNormalizer(properties.getTimeSlotWidth());
    hashFunction = Hashing.murmur3_32();
  }

  public Flux<String> getTimeSlots() {
    final String now = Long.toString(Instant.now().getEpochSecond());
    final String lastTouchDelay = Long.toString(properties.getLastTouchDelay().getSeconds());
    return redisTemplate.execute(
            this.redisGetTimeSlots, List.of(), List.of(now, lastTouchDelay)).flatMapIterable(list -> list);
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }

    final Instant normalizedTimeSlot = timestamp.with(timeSlotNormalizer);
    final String pendingValue = encodingPendingValue(tenant, seriesSetHash);
    final String timeslot = Long.toString(normalizedTimeSlot.getEpochSecond());

    return redisTemplate.opsForSet()
            .add(PREFIX_PENDING, timeslot)
            .and(redisTemplate.opsForSet().add(timeslot, pendingValue));
  }

  public Flux<PendingDownsampleSet> retrieveTimeSlots() {
    return getTimeSlots().flatMap(timeslot -> getDownsampleSets(timeslot));
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot) {
    log.info("Downsampling timeslot: {}...", timeslot);
    return redisTemplate.opsForSet()
            .scan(timeslot)
            .map(pendingValue -> buildPending(timeslot, pendingValue));
  }

  public Mono<?> complete(PendingDownsampleSet entry) {
    String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    return redisTemplate.opsForSet()
            .remove(timeslot, encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash()));
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant + DELIM + seriesSet;
  }

  private static String encodeKey(String prefix, Instant timeSlot) {
    return prefix + DELIM + timeSlot.getEpochSecond();
  }

  private static PendingDownsampleSet buildPending(String timeslot, String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(Long.parseLong(timeslot)));
  }
}
