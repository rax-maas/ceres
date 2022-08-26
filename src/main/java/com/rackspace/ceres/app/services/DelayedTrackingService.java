/*
 * Copyright 2022 Rackspace US, Inc.
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

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

import static com.rackspace.ceres.app.utils.DateTimeUtils.epochToLocalDateTime;
import static com.rackspace.ceres.app.utils.DateTimeUtils.nowEpochSeconds;

@Service
@Slf4j
@Profile("downsample")
public class DelayedTrackingService {
  private final DownsampleProperties properties;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetDelayedJob;

  @Autowired
  public DelayedTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                DownsampleProperties properties,
                                RedisScript<String> redisGetDelayedJob) {
    this.redisTemplate = redisTemplate;
    this.redisGetDelayedJob = redisGetDelayedJob;
    this.properties = properties;
  }

  public Flux<String> claimJob(Integer partition) {
    String hostName = System.getenv("HOSTNAME");
    long now = Instant.now().getEpochSecond();
    long maxJobDuration = properties.getMaxDownsampleDelayedJobDuration().getSeconds();
    return redisTemplate.execute(
        this.redisGetDelayedJob,
        List.of(),
        List.of(partition.toString(),
            hostName == null ? "localhost" : hostName,
            Long.toString(now),
            Long.toString(maxJobDuration))
    );
  }

  public Mono<?> freeJob(int partition) {
    log.trace("free job partition {}", partition);
    return redisTemplate.opsForValue().set(encodeJobKey(partition), "free");
  }

  public Flux<String> getDelayedTimeSlots(Integer partition, String group) {
    String key = encodeDelayedTimeslotKey(partition, group);
    return this.redisTemplate.opsForSet().scan(key)
        .filter(ts -> isNotInProgress(ts, partition))
        .concatMap(timeslot -> isInProgress(timeslot) ? Mono.just(timeslot) :
            this.redisTemplate.opsForSet().remove(key, timeslot)
                .then(this.redisTemplate.opsForSet().add(key, encodeTimeslotInProgress(timeslot)))
                .then(Mono.just(timeslot))
        );
  }


  public boolean isInProgress(String timeslot) {
    return timeslot.matches(".*in-progress");
  }

  public boolean isNotInProgress(String timeslot, int partition) {
    boolean isNotInProgress = true;
    if (timeslot.matches(".*in-progress")) {
      long tsLongValue = Long.parseLong(timeslot.split("\\|")[0]);
      if (tsLongValue < (nowEpochSeconds() - properties.getMaxDelayedInProgress().getSeconds())) {
        // This delayed timeslot is hanging in state in-progress
        log.warn("Delayed in-progress timeslot is hanging: {} {}", partition, epochToLocalDateTime(tsLongValue));
        // It's hanging so we consider it not in-progress
      } else {
        isNotInProgress = false;
      }
    }
    return isNotInProgress;
  }

  public Flux<String> getDelayedHashes(int partition) {
    String key = encodeDelayedHashesKey(partition);
    return this.redisTemplate.opsForSet().scan(key);
  }

  public Mono<?> deleteDelayedTimeslot(Integer partition, String group, Long timeslot) {
    return redisTemplate.opsForSet()
        .remove(encodeDelayedTimeslotKey(partition, group), encodeTimeslotInProgress(timeslot))
        .flatMap(result -> {
              log.trace("Deleted delayed timeslot result: {}, {} {}", result, partition, epochToLocalDateTime(timeslot));
              return Mono.just(result);
            }
        );
  }

  public Mono<?> deleteDelayedHashes(Integer partition) {
    return redisTemplate.opsForSet()
        .delete(encodeDelayedHashesKey(partition))
        .flatMap(result -> {
              log.trace("Deleted delayed hashes result: {}, {}", result, partition);
              return Mono.just(result);
            }
        );
  }

  private static String encodeTimeslotInProgress(long timeslot) {
    return String.format("%d|in-progress", timeslot);
  }

  private static String encodeTimeslotInProgress(String timeslot) {
    return String.format("%s|in-progress", timeslot);
  }

  private static String encodeJobKey(int partition) {
    return String.format("job|%d", partition);
  }

  private static String encodeDelayedTimeslotKey(int partition, String group) {
    return String.format("delayed|%d|%s", partition, group);
  }

  private static String encodeDelayedHashesKey(int partition) {
    return String.format("delayed-hashes|%d", partition);
  }

  public static String encodeDelayedTimeslot(PendingDownsampleSet set) {
    return String.format("%d|%s|%s", set.getTimeSlot().getEpochSecond(), set.getTenant(), set.getSeriesSetHash());
  }

  public static PendingDownsampleSet buildDelayedPending(String hash, Long timeslot) {
    log.trace("Build delayed pending: {}", hash);
    String[] tokens = hash.split("\\|");
    return new PendingDownsampleSet()
        .setTimeSlot(Instant.ofEpochSecond(timeslot))
        .setTenant(tokens[0])
        .setSeriesSetHash(tokens[1]);
  }
}
