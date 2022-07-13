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

import com.rackspace.ceres.app.config.AppProperties;
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

import static com.rackspace.ceres.app.utils.DateTimeUtils.*;

@Service
@Slf4j
@Profile("downsample")
public class DelayedTrackingService {
  private final DownsampleProperties properties;
  private final AppProperties appProperties;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetDelayedJob;

  @Autowired
  public DelayedTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                DownsampleProperties properties,
                                AppProperties appProperties,
                                RedisScript<String> redisGetDelayedJob) {
    this.redisTemplate = redisTemplate;
    this.redisGetDelayedJob = redisGetDelayedJob;
    this.properties = properties;
    this.appProperties = appProperties;
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
        .filter(ts -> isNotInProgress(ts, partition, group))
        .flatMap(timeslot -> isInProgress(timeslot) ? Mono.just(timeslot) :
            this.redisTemplate.opsForSet().remove(key, timeslot)
                .then(this.redisTemplate.opsForSet().add(key, encodeTimeslotInProgress(timeslot)))
                .then(Mono.just(timeslot))
        );
  }


  public boolean isInProgress(String timeslot) {
    return timeslot.matches(".*in-progress");
  }

  public boolean isNotInProgress(String timeslot, int partition, String group) {
    boolean isNotInProgress = true;
    if (timeslot.matches(".*in-progress")) {
      long tsLongValue = Long.parseLong(timeslot.split("\\|")[0]);
      if (tsLongValue < (nowEpochSeconds() - appProperties.getIngestStartTime().getSeconds())) {
        // This delayed timeslot is hanging in state in-progress
        log.warn("Delayed in-progress timeslot is hanging: {} {} {}",
            partition, group, epochToLocalDateTime(tsLongValue));
        // It's hanging so we consider it not in-progress
      } else {
        isNotInProgress = false;
      }
    }
    return isNotInProgress;
  }

  public Mono<?> deleteDelayedTimeslot(Integer partition, String group, String timeslot) {
    String[] tsArray = timeslot.split("\\|");
    long ts = Long.parseLong(tsArray[0]);
    return redisTemplate.opsForSet()
        .remove(encodeDelayedTimeslotKey(partition, group), encodeTimeslotInProgress(timeslot))
        .flatMap(result -> {
              log.trace("Deleted delayed timeslot result: {}, {} {} {}", result, partition, group, epochToLocalDateTime(ts));
              return Mono.just(result);
            }
        );
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

  public static PendingDownsampleSet buildDownsampleSet(int partition, String group, String timeslot) {
    String [] tsArray = timeslot.split("\\|");
    long ts = Long.parseLong(tsArray[0]);
    log.trace("Got delayed timeslot: {} {} {}", partition, group, epochToLocalDateTime(ts));
    String tenant = tsArray[1];
    String setHash = tsArray[2];
    return new PendingDownsampleSet()
        .setTenant(tenant)
        .setSeriesSetHash(setHash)
        .setTimeSlot(Instant.ofEpochSecond(ts));
  }

  public static String encodeDelayedTimeslot(PendingDownsampleSet set) {
    return String.format("%d|%s|%s", set.getTimeSlot().getEpochSecond(), set.getTenant(), set.getSeriesSetHash());
  }
}
