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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.hash.HashCode;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.Job;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import com.rackspace.ceres.app.utils.JobUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final String DELIM = "|";
  private static final String PREFIX_PENDING = "pending";
  private static final String PREFIX_DOWNSAMPLING = "downsampling";

  private final ReactiveStringRedisTemplate redisTemplate;
  private final DownsampleProperties properties;
  private final HashService hashService;
  private final JobUtils jobUtils;

  @Autowired
  public DownsampleTrackingService(ReactiveStringRedisTemplate redisTemplate,
                                   DownsampleProperties properties,
                                   HashService hashService, JobUtils jobUtils) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    this.jobUtils = jobUtils;
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    this.hashService = hashService;
  }

  public Flux<String> getTimeSlot(Integer partition, String group) {
    long epochNow = Instant.now().getEpochSecond();
    long partitionWidthSeconds = Duration.parse(group).getSeconds();
    return redisTemplate
            .opsForSet().members(PREFIX_PENDING + DELIM + partition + DELIM + group)
            // check if we are within downsampling width, i.e. if we are due
            .filter(timeslotEpoch -> Long.valueOf(timeslotEpoch).longValue() + partitionWidthSeconds < epochNow)
            .take(1); // Only one timeslot at a time
  }

  public Mono<String> checkPartitionJob(Integer partition, String group) {
    log.info("checkPartitionJob is {} {}", partition, group);
    InetAddress inetAddress = null;
    try {
      inetAddress = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    Job job = new Job(partition, group, inetAddress.getHostName());
    String body = null;
    try {
      body = new ObjectMapper().writeValueAsString(job);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    log.info("hostname and IP are {} {}", inetAddress.getHostName(), inetAddress.getHostAddress());

    if (inetAddress.getHostName().equals(properties.getJobsHost()) ||
            inetAddress.getHostAddress().equals(properties.getJobsHost())) {
      log.info("hostname and IP are {} {} just calling internal", inetAddress.getHostName(), inetAddress.getHostAddress());
      return Mono.just(this.jobUtils.getJobInternal(job));
    } else {
      log.info("hostname is {} calling rest api", inetAddress.getHostName());
      WebClient webClient = WebClient.create(
              "http://" + properties.getJobsHost() + ":" + properties.getJobsPort() + "/api/job");
      return webClient.post()
              .accept(MediaType.APPLICATION_JSON)
              .contentType(MediaType.APPLICATION_JSON)
              .body(BodyInserters.fromObject(body))
              .retrieve()
              .bodyToMono(String.class);
    }
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
              return redisTemplate.opsForSet()
                              .add(PREFIX_PENDING + DELIM + partition + DELIM + width, timeslot)
                              .and(redisTemplate.opsForSet().add(downsamplingTimeslot, pendingValue));
            }
    );
  }

  public Flux<PendingDownsampleSet> retrieveDownsampleSets(int partition, String group) {
    return getTimeSlot(partition, group)
            .flatMap(timeslot -> timeslot.equals("") ? Flux.empty() : getDownsampleSets(timeslot, partition, group));
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot, int partition, String group) {
    log.info("getDownsampleSets {} {} {}", timeslot, partition, group);
    final String downsamplingTimeslot = encodeDownsamplingTimeslot(timeslot, partition, group);
    return redisTemplate.opsForValue().get("set-hashes-process-limit").flatMapMany(
            processLimit -> {
              return redisTemplate.opsForSet()
                      .scan(downsamplingTimeslot)
                      .take(Long.parseLong(processLimit))
                      .map(pendingValue -> buildPending(timeslot, pendingValue));
            });
  }

  public Mono<?> initJob(int partition, String group) {
    log.info("initJob {} {}", partition, group);
    InetAddress inetAddress = null;
    try {
      inetAddress = InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
    Job job = new Job(partition, group, inetAddress.getHostName());
    log.info("hostname and IP are {} {}", inetAddress.getHostName(), inetAddress.getHostAddress());

    if (inetAddress.getHostName().equals(properties.getJobsHost()) ||
            inetAddress.getHostAddress().equals(properties.getJobsHost())) {
      log.info("hostname is {} just calling internal", inetAddress.getHostName());
      return Mono.just(this.jobUtils.freeJobInternal(job));
    } else {
      log.info("hostname is {} calling rest api", inetAddress.getHostName());
      WebClient webClient = WebClient.create(
              "http://" + properties.getJobsHost() + ":" + properties.getJobsPort() + "/api/job");
      String body = null;
      try {
        body = new ObjectMapper().writeValueAsString(job);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      return webClient.put()
              .accept(MediaType.APPLICATION_JSON)
              .contentType(MediaType.APPLICATION_JSON)
              .body(BodyInserters.fromObject(body))
              .retrieve()
              .bodyToMono(String.class);
    }
  }

  public Mono<?> setRedisSetHashesProcessLimit() {
    Long processLimit = properties.getSetHashesProcessLimit();
    return redisTemplate.opsForValue().set("set-hashes-process-limit", processLimit.toString());
  }

  public Mono<?> setRedisSpreadPeriod() {
    Long spreadPeriod = properties.getDownsampleSpreadPeriod().getSeconds();
    return redisTemplate.opsForValue().set("downsample-spread-period", spreadPeriod.toString());
  }

  public Mono<?> getRedisSpreadPeriod() {
    return redisTemplate.opsForValue().get("downsample-spread-period");
  }

  public Mono<?> complete(PendingDownsampleSet entry, Integer partition, String group) {
    final String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    final String downsamplingTimeslot = encodeDownsamplingTimeslot(timeslot, partition, group);
    final String value = encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash());
    return redisTemplate.
            opsForSet()
            .remove(downsamplingTimeslot, value)
            .then(
                    redisTemplate
                            .opsForSet()
                            .size(downsamplingTimeslot)
                            .flatMap(size -> size == 0 ?
                                    redisTemplate.opsForSet()
                                            .remove(encodePendingGroup(partition, group), timeslot) :
                                    Mono.just(true))
            );
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

  private static String encodePendingGroup(int partition, String group) {
    return PREFIX_PENDING + DELIM + partition + DELIM + group;
  }

  private static PendingDownsampleSet buildPending(String timeslot, String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(Long.parseLong(timeslot)));
  }
}
