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
import com.rackspace.ceres.app.model.Downsampling;
import com.rackspace.ceres.app.model.Job;
import com.rackspace.ceres.app.model.Pending;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.repos.DownsamplingRepository;
import com.rackspace.ceres.app.repos.PendingRepository;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final String DELIM = "|";
  private static final String PREFIX_PENDING = "pending";
  private static final String PREFIX_DOWNSAMPLING = "downsampling";

  private final DownsampleProperties properties;
  private final HashService hashService;
  private final PendingRepository pendingRepository;
  private final DownsamplingRepository downsamplingRepository;
  private final String hostname;
  private final MongoOperations mongoOperations;

  @Autowired
  public DownsampleTrackingService(DownsampleProperties properties,
                                   HashService hashService,
                                   PendingRepository pendingRepository,
                                   DownsamplingRepository downsamplingRepository,
                                   MongoOperations mongoOperations) throws UnknownHostException {
    this.properties = properties;
    this.pendingRepository = pendingRepository;
    this.downsamplingRepository = downsamplingRepository;
    this.mongoOperations = mongoOperations;
    this.hostname = InetAddress.getLocalHost().getHostName();
    log.info("Downsample tracking is {}", properties.isTrackingEnabled() ? "enabled" : "disabled");
    this.hashService = hashService;
  }

  public Flux<String> getTimeSlot(Integer partition, String group) {
    long now = Instant.now().getEpochSecond();
    long partitionWidthSeconds = Duration.parse(group).getSeconds();
    Pending pending = this.pendingRepository.findByPartitionAndGroup(partition, group);
    if (pending == null) {
      return Flux.empty();
    }
    List<String> sortedTimeslots = new ArrayList<>(pending.getTimeslots());
    if (sortedTimeslots.size() > 3) {
      log.warn("Timeslot count for partition: {} and group: {} is larger than 3.", partition, group);
    }
    Collections.sort(sortedTimeslots);
    String timeslot = sortedTimeslots.get(0);
    long then = Instant.ofEpochSecond(Long.parseLong(timeslot)).getEpochSecond();
    if (then + partitionWidthSeconds < now) {
      return Flux.just(timeslot);
    }
    return Flux.empty();
  }

  public Mono<String> checkPartitionJob(Integer partition, String group) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition)
            .and("group").is(group)
            .and("timestamp").lt(Instant.now()));
    Update update = new Update();
    update.set("status", this.hostname);
    update.set("timestamp", Instant.now().plusSeconds(5)); // TODO: fix this!

    Job job = this.mongoOperations.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), Job.class);
    if (job != null && job.getStatus().equals(this.hostname)) {
      return Mono.just("Job is assigned");
    }
    return Mono.just("Job is not free");
  }

  public void initJob(int partition, String group) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition).and("group").is(group).and("status").is(this.hostname));
    Update update = new Update();
    update.set("status", "free");

    Job job = this.mongoOperations.findAndModify(query, update, new FindAndModifyOptions().returnNew(true), Job.class);
    if (job != null && job.getStatus().equals("free")) {
      log.info("Job is free {} {}", partition, group);
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
              savePending(partition, width, timeslot);
              saveDownsampling(partition, width, timeslot, pendingValue);
              return Mono.empty();
            }
    );
  }

  private void saveDownsampling(Integer partition, String width, String timeslot, String value) {
    Downsampling downsampling =
            this.downsamplingRepository.findByPartitionAndGroupAndTimeslot(partition, width, timeslot);
    if (downsampling == null) {
      downsampling = new Downsampling(partition, width, timeslot);
    }
    downsampling.getHashes().add(value);
    this.downsamplingRepository.save(downsampling);
  }

  private void savePending(Integer partition, String width, String timeslot) {
    Pending pending = this.pendingRepository.findByPartitionAndGroup(partition, width);
    if (pending == null) {
      pending = new Pending(partition, width);
    }
    pending.getTimeslots().add(timeslot);
    this.pendingRepository.save(pending);
  }

  public Flux<PendingDownsampleSet> retrieveDownsampleSets(int partition, String group) {
    return getTimeSlot(partition, group)
            .flatMap(timeslot -> timeslot.equals("") ? Flux.empty() : getDownsampleSets(timeslot, partition, group));
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot, int partition, String group) {
    log.trace("getDownsampleSets {} {} {}", timeslot, partition, group);
    Downsampling downsampling =
            this.downsamplingRepository.findByPartitionAndGroupAndTimeslot(partition, group, timeslot);
    if (downsampling == null) {
      log.error("getDownsampleSets downsampling is not found");
      return Flux.empty();
    }
    return Flux.fromIterable(downsampling.getHashes())
            .take(properties.getSetHashesProcessLimit())
            .map(pendingValue -> buildPending(timeslot, pendingValue));
  }

  public Mono<?> complete(PendingDownsampleSet entry, Integer partition, String group) {
    final String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    final String value = encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash());
    removeHash(partition, group, timeslot, value);
    removeTimeslot(partition, group, timeslot);
    return Mono.just(true);
  }

  private void removeHash(Integer partition, String group, String timeslot, String value) {
    Downsampling downsampling =
            this.downsamplingRepository.findByPartitionAndGroupAndTimeslot(partition, group, timeslot);
    downsampling.getHashes().remove(value);
    if (downsampling.getHashes().size() == 0) {
      this.downsamplingRepository.delete(downsampling);
    } else {
      this.downsamplingRepository.save(downsampling);
    }
  }

  private void removeTimeslot(Integer partition, String group, String timeslot) {
    Pending pending = this.pendingRepository.findByPartitionAndGroup(partition, group);
    pending.getTimeslots().remove(timeslot);
    if (pending.getTimeslots().size() == 0) {
      this.pendingRepository.delete(pending);
    } else {
      this.pendingRepository.save(pending);
    }
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant + DELIM + seriesSet;
  }

  private static PendingDownsampleSet buildPending(String timeslot, String pendingValue) {
    final int splitValueAt = pendingValue.indexOf(DELIM);

    return new PendingDownsampleSet()
        .setTenant(pendingValue.substring(0, splitValueAt))
        .setSeriesSetHash(pendingValue.substring(1 + splitValueAt))
        .setTimeSlot(Instant.ofEpochSecond(Long.parseLong(timeslot)));
  }
}
