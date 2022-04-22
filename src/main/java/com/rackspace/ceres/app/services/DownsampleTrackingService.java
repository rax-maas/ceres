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
import java.time.Instant;
import java.util.List;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final String DELIM = "|";
  private static final String PREFIX_INGESTING = "ingesting";
  private static final String PREFIX_PENDING = "pending";
  private static final String PREFIX_DOWNSAMPLING = "downsampling";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<List> redisGetTimeSlots;
  private final RedisScript<String> redisGetJob;
  private final DownsampleProperties properties;
  private final TemporalNormalizer timeSlotMaxNormalizer;
  private final TemporalNormalizer timeSlotMinNormalizer;
  private final HashService hashService;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   RedisScript<List> redisGetTimeSlots,
                                   RedisScript<String> redisGetJob,
                                   DownsampleProperties properties,
                                   HashService hashService) {
    this.redisTemplate = redisTemplate;
    this.redisGetTimeSlots = redisGetTimeSlots;
    this.redisGetJob = redisGetJob;
    this.properties = properties;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    this.timeSlotMaxNormalizer = new TemporalNormalizer(properties.getTimeSlotMaxWidth());
    this.timeSlotMinNormalizer = new TemporalNormalizer(properties.getTimeSlotMinWidth());
    this.hashService = hashService;
  }

  public Flux<String> getTimeSlots(Integer partition, String group) {
    final String now = Long.toString(Instant.now().getEpochSecond());
    long widthSeconds = group.equals("max") ?
            properties.getTimeSlotMaxWidth().getSeconds() : properties.getTimeSlotMinWidth().getSeconds();
    final String timeSlotWidth = Long.toString(widthSeconds);
    return redisTemplate.execute(
            this.redisGetTimeSlots, List.of(), List.of(now, timeSlotWidth, partition.toString(), group)
    ).flatMapIterable(list -> list);
  }

  public Flux<String> checkPartitionJob(Integer partition, String group) {
    String hostName = System.getenv("HOSTNAME");
    final String now = Long.toString(Instant.now().getEpochSecond());
    return redisTemplate.execute(
            this.redisGetJob,
            List.of(),
            List.of(partition.toString(), group, hostName == null ? "localhost" : hostName, now));
  }

  public Publisher<?> track(String tenant, String seriesSetHash, Instant timestamp) {
    if (!properties.isTrackingEnabled()) {
      return Mono.empty();
    }

    final HashCode hashCode = hashService.getHashCode(tenant, seriesSetHash);
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());
//    log.info("partition: {}", partition);

    final Instant normalizedMaxTimeSlot = timestamp.with(this.timeSlotMaxNormalizer);
    final Instant normalizedMinTimeSlot = timestamp.with(this.timeSlotMinNormalizer);
    final String pendingValue = encodingPendingValue(tenant, seriesSetHash);
    final String timeslotMax = Long.toString(normalizedMaxTimeSlot.getEpochSecond());
    final String timeslotMin = Long.toString(normalizedMinTimeSlot.getEpochSecond());
    final String downsamplingTimeslotMax = encodeDownsamplingTimeslot(timeslotMax, partition, "max");
    final String downsamplingTimeslotMin = encodeDownsamplingTimeslot(timeslotMin, partition, "min");
    final String ingestingKeyMax = encodeKey(PREFIX_INGESTING, partition, "max", timeslotMax);
    final String ingestingKeyMin = encodeKey(PREFIX_INGESTING, partition, "min", timeslotMin);

    return redisTemplate.opsForValue()
            .set(ingestingKeyMax, "", properties.getLastTouchDelay())
            .and(redisTemplate.opsForValue().set(ingestingKeyMin, "", properties.getLastTouchDelay()))
            .and(redisTemplate.opsForSet()
                    .add(PREFIX_PENDING + DELIM + partition + DELIM + "max", timeslotMax)
                    .and(redisTemplate.opsForSet().add(downsamplingTimeslotMax, pendingValue)))
            .and(redisTemplate.opsForSet()
                    .add(PREFIX_PENDING + DELIM + partition + DELIM + "min", timeslotMin)
                    .and(redisTemplate.opsForSet().add(downsamplingTimeslotMin, pendingValue)));
  }

  public Flux<PendingDownsampleSet> retrieveTimeSlots(int partition, String group) {
    return getTimeSlots(partition, group).flatMap(timeslot -> getDownsampleSets(timeslot, partition, group));
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot, int partition, String group) {
    // log.info("Downsampling timeslot: {} partition: {}", timeslot, partition);
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
