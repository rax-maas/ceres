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
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final String DELIM = "|";
  private static final String PREFIX_INGESTING = "ingesting";
  private static final String PREFIX_PENDING = "pending";
  private static final String PREFIX_DOWNSAMPLING = "downsampling";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetTimeSlot;
  private final RedisScript<String> redisGetJob;
  private final RedisScript<String> redisCheckOldTimeSlots;
  private final DownsampleProperties properties;
  private final HashService hashService;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   RedisScript<String> redisGetTimeSlot,
                                   RedisScript<String> redisGetJob,
                                   RedisScript<String> redisCheckOldTimeSlots,
                                   DownsampleProperties properties,
                                   HashService hashService) {
    this.redisTemplate = redisTemplate;
    this.redisGetTimeSlot = redisGetTimeSlot;
    this.redisGetJob = redisGetJob;
    this.redisCheckOldTimeSlots = redisCheckOldTimeSlots;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    this.hashService = hashService;
  }

  public Flux<String> getTimeSlot(Integer partition, String group) {
    final String now = Long.toString(Instant.now().getEpochSecond());
    final String timeSlotWidth = Long.toString(Duration.parse(group).getSeconds());
    return redisTemplate.execute(
            this.redisGetTimeSlot, List.of(), List.of(now, timeSlotWidth, partition.toString(), group));
  }

  public Flux<String> checkPartitionJob(Integer partition, String group) {
    String hostName = System.getenv("HOSTNAME");
    final String now = Long.toString(Instant.now().getEpochSecond());
    return redisTemplate.execute(
            this.redisGetJob,
            List.of(),
            List.of(partition.toString(), group, hostName == null ? "localhost" : hostName, now));
  }

  public Flux<String> checkOldTimeSlots() {
    final String now = Long.toString(Instant.now().getEpochSecond());
    final String partitionWidths = String.join("|", DateTimeUtils.getPartitionWidths(properties.getGranularities()));
    final Integer partitions = properties.getPartitions() - 1;
    final Long maxDownsamplingTime = properties.getMaxDownsamplingTime().getSeconds();
    return redisTemplate.execute(
            this.redisCheckOldTimeSlots, List.of(), List.of(
                    now, partitionWidths, partitions.toString(), maxDownsamplingTime.toString()));
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
              final String timeslot = Long.toString(normalizedTimeSlot.getEpochSecond());
              final String downsamplingTimeslot = encodeDownsamplingTimeslot(timeslot, partition, width);
              final String ingestingKey = encodeKey(PREFIX_INGESTING, partition, width, timeslot);
              return redisTemplate.opsForValue()
                      .set(ingestingKey, "", properties.getLastTouchDelay())
                      .and(redisTemplate.opsForSet()
                              .add(PREFIX_PENDING + DELIM + partition + DELIM + width, timeslot)
                              .and(redisTemplate.opsForSet().add(downsamplingTimeslot, pendingValue)));
            }
    );
  }

  public Flux<PendingDownsampleSet> retrieveDownsampleSets(int partition, String group) {
    return getTimeSlot(partition, group)
            .flatMap(timeslot -> timeslot.equals("") ? Flux.empty() : getDownsampleSets(timeslot, partition, group));
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot, int partition, String group) {
//    log.info("Downsampling timeslot: {} partition: {} group: {}", timeslot, partition, group);
    final String downsamplingTimeslot = encodeDownsamplingTimeslot(timeslot, partition, group);
    return redisTemplate.opsForSet()
            .scan(downsamplingTimeslot)
            .map(pendingValue -> buildPending(timeslot, pendingValue));
  }

  public Mono<?> initJob(int partition, String group) {
    return redisTemplate.opsForValue().set("job|" + partition + "|" + group, "free");
  }

  public Mono<?> complete(PendingDownsampleSet entry, int partition, String group) {
    final String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    final String downsamplingTimeslot = encodeDownsamplingTimeslot(timeslot, partition, group);
    final String value = encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash());
    return redisTemplate.opsForSet().remove(downsamplingTimeslot, value);
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant + DELIM + seriesSet;
  }

  private static String encodeKey(String prefix, int partition, String group, String timeSlot) {
    return prefix + DELIM + partition + DELIM + group + DELIM + timeSlot;
  }

  private static String encodeDownsamplingTimeslot(String timeslot, int partition, String group) {
    return PREFIX_DOWNSAMPLING + DELIM + partition + DELIM + group + DELIM + timeslot;
  }

  private static PendingDownsampleSet buildPending(String timeslot, String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(Long.parseLong(timeslot)));
  }
}
