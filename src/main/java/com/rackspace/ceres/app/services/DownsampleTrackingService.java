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
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.model.Timeslot;
import com.rackspace.ceres.app.repos.DownsamplingRepository;
import com.rackspace.ceres.app.repos.TimeslotRepository;
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
import java.time.ZoneId;

@SuppressWarnings("UnstableApiUsage") // guava
@Service
@Slf4j
public class DownsampleTrackingService {

  private static final String DELIM = "|";

  private final DownsampleProperties properties;
  private final HashService hashService;
  private final WebClientUtils webClientUtils;
  private final ReactiveMongoOperations mongoOperations;
  private final DownsamplingRepository downsamplingRepository;
  private final TimeslotRepository timeslotRepository;

  @Autowired
  public DownsampleTrackingService(DownsampleProperties properties,
                                   HashService hashService,
                                   WebClientUtils webClientUtils,
                                   ReactiveMongoOperations mongoOperations,
                                   DownsamplingRepository downsamplingRepository,
                                   TimeslotRepository timeslotRepository) {
    this.properties = properties;
    this.hashService = hashService;
    this.webClientUtils = webClientUtils;
    this.mongoOperations = mongoOperations;
    this.downsamplingRepository = downsamplingRepository;
    this.timeslotRepository = timeslotRepository;
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
    final int partition = hashService.getPartition(hashCode, properties.getPartitions());

    return Flux.fromIterable(DateTimeUtils.getPartitionWidths(properties.getGranularities())).flatMap(
        width -> {
          final Instant normalizedTimeSlot = timestamp.with(new TemporalNormalizer(Duration.parse(width)));
          final String setHash = encodingPendingValue(tenant, seriesSetHash);
          return saveDownsampling(partition, width, normalizedTimeSlot, setHash);
        }
    );
  }

  private Mono<?> saveDownsampling(Integer partition, String group, Instant timeslot, String setHash) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").is(timeslot)
        .and("setHash").is(setHash));
    Update update = new Update();
    update.set("partition", partition)
        .set("group", group)
        .set("timeslot", timeslot)
        .set("setHash", setHash);
    FindAndModifyOptions options = new FindAndModifyOptions();
    options.returnNew(true).upsert(true);

    Query query1 = new Query();
    query1.addCriteria(Criteria.where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").is(timeslot));
    Update update1 = new Update();
    update1.set("partition", partition)
        .set("group", group)
        .set("timeslot", timeslot);
    FindAndModifyOptions options1 = new FindAndModifyOptions();
    options1.returnNew(true).upsert(true);
    return this.mongoOperations.findAndModify(query, update, options, Downsampling.class)
        .then(this.mongoOperations.findAndModify(query1, update1, options1, Timeslot.class))
        .name("saveDownsampling")
        .metrics();
  }

  public Flux<PendingDownsampleSet> getDownsampleSets(int partition, String group) {
    log.trace("getDownsampleSets {} {}", partition, group);
    long partitionWidth = Duration.parse(group).getSeconds();
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition).and("group").is(group)
        .and("timeslot").lt(Instant.now().minusSeconds(partitionWidth)));
    query.fields().include("timeslot");
    return this.mongoOperations.findOne(query, Timeslot.class)
        .name("getDownsampleSets")
        .metrics()
        .flatMapMany(
            d2 -> {
              log.info("Got timeslot: {} {} {}",
                  d2.getTimeslot().atZone(ZoneId.systemDefault()).toLocalDateTime(), partition, group);
              Query query1 = new Query();
              query1.addCriteria(Criteria.where("partition").is(partition)
                  .and("group").is(group).and("timeslot").is(d2.getTimeslot()));
              query1.fields().include("setHash");
              Flux<Downsampling> flux = this.mongoOperations.find(query1, Downsampling.class);
              return flux.hasElements().flatMapMany(hasElements -> {
                    if (hasElements) {
                      log.info("Got hashes...");
                      return flux.take(properties.getSetHashesProcessLimit())
                          .map(d3 -> buildPending(d2.getTimeslot(), d3.getSetHash()));
                    } else {
                      log.info("Empty hashes...");
                      return this.timeslotRepository.deleteByPartitionAndGroupAndTimeslot(partition, group, d2.getTimeslot())
                          .flatMapMany(o -> Flux.empty());
                    }
                  }
              );
            });
  }

  public Mono<?> complete(PendingDownsampleSet entry, Integer partition, String group) {
    final String setHash = encodingPendingValue(entry.getTenant(), entry.getSeriesSetHash());
    return deleteDownsampling(partition, group, entry.getTimeSlot(), setHash);
  }

  private Mono<?> deleteDownsampling(Integer partition, String group, Instant timeslot, String setHash) {
    log.trace("Delete downsampling: {} {} {} {}", partition, group, timeslot, setHash);
    return this.downsamplingRepository.deleteByPartitionAndGroupAndTimeslotAndSetHash(partition, group, timeslot, setHash)
            .name("removeHash")
            .metrics();
  }

  private static String encodingPendingValue(String tenant, String seriesSet) {
    return tenant + DELIM + seriesSet;
  }

  private static PendingDownsampleSet buildPending(Instant timeslot, String pendingValue) {
    log.trace("buildPending {}", pendingValue);
    String[] values = pendingValue.split("\\|");

    return new PendingDownsampleSet()
        .setTimeSlot(timeslot)
        .setTenant(values[0])
        .setSeriesSetHash(values[1]);
  }
}
