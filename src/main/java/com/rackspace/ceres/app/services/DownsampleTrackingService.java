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
import com.rackspace.ceres.app.model.Pending;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.repos.DownsamplingRepository;
import com.rackspace.ceres.app.repos.PendingRepository;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import com.rackspace.ceres.app.utils.WebClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

  private final DownsampleProperties properties;
  private final HashService hashService;
  private final PendingRepository pendingRepository;
  private final DownsamplingRepository downsamplingRepository;
  private final WebClientUtils webClientUtils;
  private final ReactiveMongoOperations mongoOperations;

  @Autowired
  public DownsampleTrackingService(DownsampleProperties properties,
                                   DownsamplingRepository downsamplingRepository,
                                   PendingRepository pendingRepository,
                                   HashService hashService, WebClientUtils webClientUtils,
                                   ReactiveMongoOperations mongoOperations) {
    this.properties = properties;
    this.pendingRepository = pendingRepository;
    this.downsamplingRepository = downsamplingRepository;
    this.hashService = hashService;
    this.webClientUtils = webClientUtils;
    this.mongoOperations = mongoOperations;
  }

  public Mono<String> getTimeSlot(Integer partition, String group) {
    long now = Instant.now().getEpochSecond();
    long partitionWidth = Duration.parse(group).getSeconds();
    return this.pendingRepository.findByPartitionAndGroup(partition, group)
        .flatMap(pending -> {
          List<String> sortedTimeslots = new ArrayList<>(pending.getTimeslots());
          if (sortedTimeslots.size() > 3) {
            log.warn("Timeslot count for partition: {} and group: {} is larger than 3.", partition, group);
          }
          Collections.sort(sortedTimeslots);
          return Flux.fromIterable(sortedTimeslots)
              .filter(timeslot -> isDueTimeslot(timeslot, partitionWidth, now))
              .next();
        });
  }

  private boolean isDueTimeslot(String timeslot, long partitionWidth, long now) {
    return Instant.ofEpochSecond(Long.parseLong(timeslot)).getEpochSecond() + partitionWidth < now;
  }

  public Mono<String> claimJob(Integer partition, String group) {
    return this.webClientUtils.claimJob(partition, group);
  }

  public Mono<?> freeJob(int partition, String group) {
    return this.webClientUtils.freeJob(partition, group);
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
          return savePending(partition, width, timeslot)
              .and(saveDownsampling(partition, width, timeslot, pendingValue));
        }
    );
  }

  private Mono<?> saveDownsampling(Integer partition, String group, String timeslot, String value) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition).and("group").is(group).and("timeslot").is(timeslot));
    Update update = new Update();
    update.addToSet("hashes", value);
    FindAndModifyOptions options = new FindAndModifyOptions();
    options.returnNew(true).upsert(true);

    return this.mongoOperations.findAndModify(query, update, options, Downsampling.class)
        .flatMap(downsampling -> {
          log.trace("updated downsampling: {}", downsampling);
          return Mono.empty();
        });
  }

  private Mono<?> savePending(Integer partition, String group, String timeslot) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition).and("group").is(group));
    Update update = new Update();
    update.addToSet("timeslots", timeslot);
    FindAndModifyOptions options = new FindAndModifyOptions();
    options.returnNew(true).upsert(true);

    return this.mongoOperations.findAndModify(query, update, options, Pending.class)
        .flatMap(pending -> {
          log.trace("updated pending: {}", pending);
          return Mono.empty();
        });
  }

  public Flux<PendingDownsampleSet> retrieveDownsampleSets(int partition, String group) {
    return getTimeSlot(partition, group)
        .flatMapMany(timeslot -> getDownsampleSets(timeslot, partition, group));
  }

  private Flux<PendingDownsampleSet> getDownsampleSets(String timeslot, int partition, String group) {
    log.trace("getDownsampleSets {} {} {}", timeslot, partition, group);
    return this.downsamplingRepository.findByPartitionAndGroupAndTimeslot(partition, group, timeslot)
        .flatMapMany(
            downsampling -> Flux.fromIterable(downsampling.getHashes())
                .take(properties.getSetHashesProcessLimit())
                .map(pendingValue -> buildPending(timeslot, pendingValue))
        );
  }

  public Mono<?> complete(PendingDownsampleSet entry, Integer partition, String group) {
    final String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    final String value = encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash());
    return removeHash(partition, group, timeslot, value)
        .and(removeTimeslot(partition, group, timeslot));
  }

  private Mono<?> removeHash(Integer partition, String group, String timeslot, String value) {
    return this.downsamplingRepository.findByPartitionAndGroupAndTimeslot(partition, group, timeslot)
        .flatMap(downsampling -> {
              downsampling.getHashes().remove(value);
              return (downsampling.getHashes().size() == 0) ?
                  this.downsamplingRepository.delete(downsampling) :
                  this.downsamplingRepository.save(downsampling);
            }
        );
  }

  private Mono<?> removeTimeslot(Integer partition, String group, String timeslot) {
    return this.pendingRepository.findByPartitionAndGroup(partition, group)
        .flatMap(pending -> {
          pending.getTimeslots().remove(timeslot);
          return (pending.getTimeslots().size() == 0) ? this.pendingRepository.delete(pending) :
              this.pendingRepository.save(pending);
        });
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
