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

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.rackspace.ceres.app.utils.DateTimeUtils.nowEpochSeconds;
import static com.rackspace.ceres.app.utils.DateTimeUtils.epochToLocalDateTime;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
@Profile("downsample")
public class DownsampleTrackingService {

  private final DownsampleProperties properties;
  private final AppProperties appProperties;
  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveStringRedisTemplate redisTemplate;
  private final RedisScript<String> redisGetJob;

  private static final String GET_HASHES_TO_DOWNSAMPLE = "SELECT hash FROM downsampling_hashes WHERE partition = ?";

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   RedisScript<String> redisGetJob,
                                   DownsampleProperties properties,
                                   AppProperties appProperties,
                                   ReactiveCqlTemplate cqlTemplate) {
    this.redisTemplate = redisTemplate;
    this.redisGetJob = redisGetJob;
    this.properties = properties;
    this.appProperties = appProperties;
    this.cqlTemplate = cqlTemplate;
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
        .filter(t -> isTimeslotDue(t, partitionWidth))
        .next();
  }

  public Mono<String> getDelayedTimeSlot(Integer partition, String group) {
    String key = encodeDelayedTimeslotKey(partition, group);
    return this.redisTemplate.opsForSet().scan(key)
        .sort() // Make sure oldest timeslot is first
        .filter(ts -> isDelayedTimeslotReady(ts, partition, group))
        .next()
        .flatMap(timeslot -> this.redisTemplate.opsForSet().remove(key, timeslot)
            .then(this.redisTemplate.opsForSet().add(key, encodeTimeslotInProgress(timeslot)))
            .then(Mono.just(timeslot))
        );
  }

  private boolean isTimeslotDue(String timeslot, long partitionWidth) {
    return Long.parseLong(timeslot) < nowEpochSeconds() - partitionWidth;
  }

  private boolean isDelayedTimeslotReady(String timeslot, int partition, String group) {
    if (isInProgress(timeslot)) {
      long ts = Long.parseLong(timeslot.split("\\|")[0]);
      if (ts < (nowEpochSeconds() - appProperties.getIngestStartTime().getSeconds())) {
        // This delayed timeslot is hanging in state in-progress
        log.warn("Setting delayed in-progress timeslot back to pending: {} {} {}",
            partition, group, epochToLocalDateTime(ts));
        String key = encodeDelayedTimeslotKey(partition, group);
        this.redisTemplate.opsForSet().remove(key, timeslot)
            .and(this.redisTemplate.opsForSet().add(key, timeslot.replace("|in-progress", "")))
            .subscribe();
      }
      return false;
    } else {
      return true;
    }
  }

  private boolean isInProgress(String timeslot) {
    return timeslot.matches(".*in-progress");
  }

  public Mono<?> deleteTimeslot(Integer partition, String group, Long timeslot) {
    return redisTemplate.opsForSet().remove(encodeTimeslotKey(partition, group), timeslot.toString())
        .flatMap(result -> {
              log.info("Deleted timeslot: {} {} {} {}", result, partition, group, epochToLocalDateTime(timeslot));
              return Mono.just(result);
            }
        );
  }

  public Mono<?> deleteDelayedTimeslot(Integer partition, String group, String timeslot) {
    String [] tsArray = timeslot.split("\\|");
    long ts = Long.parseLong(tsArray[0]);
    return redisTemplate.opsForSet()
        .remove(encodeDelayedTimeslotKey(partition, group), encodeTimeslotInProgress(timeslot))
        .flatMap(result -> {
              log.info("Deleted delayed timeslot: {} {} {} {}", result, partition, group, epochToLocalDateTime(ts));
              return Mono.just(result);
            }
        );
  }

  public Flux<PendingDownsampleSet> getDownsampleSets(Long timeslot, int partition) {
    log.trace("getDownsampleSets {} {}", timeslot, partition);
    return this.cqlTemplate.queryForFlux(GET_HASHES_TO_DOWNSAMPLE, String.class, partition)
        .map(setHash -> buildPending(timeslot, setHash));
  }

  private static String encodeTimeslotKey(int partition, String group) {
    return String.format("pending|%d|%s", partition, group);
  }

  private static String encodeTimeslotInProgress(String timeslot) {
    return String.format("%s|in-progress", timeslot);
  }

  private static String encodeJobKey(int partition, String group) {
    return String.format("job|%d|%s", partition, group);
  }

  private static String encodeDelayedTimeslotKey(int partition, String group) {
    return String.format("delayed|%d|%s", partition, group);
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
