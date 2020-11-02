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

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final Charset HASHING_CHARSET = StandardCharsets.UTF_8;
  private static final String DELIM = "|";
  private static final String PREFIX_INGESTING = "ingesting";
  private static final String PREFIX_PENDING = "pending";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final DownsampleProperties properties;
  private final TemporalNormalizer timeSlotNormalizer;
  private final HashFunction hashFunction;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   DownsampleProperties properties) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    timeSlotNormalizer = new TemporalNormalizer(properties.getTimeSlotWidth());
    hashFunction = Hashing.murmur3_32();
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }

    final HashCode hashCode = hashFunction.newHasher()
        .putString(tenant, HASHING_CHARSET)
        .putString(seriesSetHash, HASHING_CHARSET)
        .hash();
    final int partition = Hashing.consistentHash(hashCode, properties.getPartitions());
    final Instant normalizedTimeSlot = timestamp.with(timeSlotNormalizer);

    final String ingestingKey = encodeKey(PREFIX_INGESTING, partition, normalizedTimeSlot);

    final String pendingKey = encodeKey(PREFIX_PENDING, partition, normalizedTimeSlot);
    final String pendingValue = encodingPendingValue(tenant, seriesSetHash);

    return redisTemplate.opsForValue()
        .set(ingestingKey, "", properties.getLastTouchDelay())
        .and(
            redisTemplate.opsForSet()
            .add(pendingKey, pendingValue)
        );
  }

  public Flux<PendingDownsampleSet> retrieveReadyOnes(int partition) {
    return redisTemplate
        // scan over pending timeslots
        .scan(
            ScanOptions.scanOptions()
                .match(PREFIX_PENDING + DELIM + partition + DELIM + "*")
                .build()
        )
        // expand each of those
        .flatMap(pendingKey ->
            // ...first see if the ingestion key
            redisTemplate.hasKey(
                PREFIX_INGESTING + pendingKey.substring(PREFIX_PENDING.length())
            )
                // ...has gone idle and been expired away
                .filter(stillIngesting -> !stillIngesting)
                // ...otherwise, expand by popping the downsample sets from that timeslot
                .flatMapMany(ready -> expandPendingSets(partition, pendingKey))
        );
  }

  public Mono<?> complete(PendingDownsampleSet entry) {
    return redisTemplate.opsForSet()
        .remove(
            encodeKey(PREFIX_PENDING, entry.getPartition(), entry.getTimeSlot()),
            encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash())
        );
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant
        + DELIM + seriesSet;
  }

  private static String encodeKey(String prefix, int partition, Instant timeSlot) {
    return prefix
        + DELIM + partition
        + DELIM + timeSlot.getEpochSecond();
  }

  private static PendingDownsampleSet buildPending(int partition, String pendingKey,
                                                   String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setPartition(partition)
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1+splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(
            Long.parseLong(pendingKey.substring(1 + pendingKey.lastIndexOf(DELIM)))
        ));
  }

  private Flux<PendingDownsampleSet> expandPendingSets(int partition, String pendingKey) {
    return redisTemplate.opsForSet()
        .scan(pendingKey)
        .map(pendingValue -> buildPending(partition, pendingKey, pendingValue));
  }

}
