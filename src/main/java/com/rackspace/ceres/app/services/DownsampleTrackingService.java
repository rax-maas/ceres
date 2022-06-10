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
import com.rackspace.ceres.app.utils.DateTimeUtils;
import com.rackspace.ceres.app.utils.WebClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
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

  @Autowired
  public DownsampleTrackingService(DownsampleProperties properties,
                                   HashService hashService,
                                   WebClientUtils webClientUtils,
                                   ReactiveMongoOperations mongoOperations) {
    this.properties = properties;
    this.hashService = hashService;
    this.webClientUtils = webClientUtils;
    this.mongoOperations = mongoOperations;
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
    Query dQuery = downsamplingQuery(partition, group, timeslot, setHash);
    Update dUpdate = new Update();
    dUpdate.set("partition", partition).set("group", group).set("timeslot", timeslot).set("setHash", setHash);

    Query tsQuery = timeslotQuery(partition, group, timeslot);
    Update tsUpdate = new Update();
    tsUpdate.set("partition", partition).set("group", group).set("timeslot", timeslot);
    return this.mongoOperations.upsert(dQuery, dUpdate, Downsampling.class)
        .then(this.mongoOperations.upsert(tsQuery, tsUpdate, Timeslot.class))
        .name("saveDownsampling")
        .metrics();
  }

  public Flux<PendingDownsampleSet> getDownsampleSets(int partition, String group) {
    log.trace("getDownsampleSets {} {}", partition, group);
    return this.mongoOperations.findOne(timeslotScanQuery(partition, group), Timeslot.class)
        .name("getDownsampleSets")
        .metrics()
        .flatMapMany(
            ts -> {
              log.info("Got timeslot: {} {} {}",
                  ts.getTimeslot().atZone(ZoneId.systemDefault()).toLocalDateTime(), partition, group);
              Query dScanQuery = downsamplingScanQuery(partition, group, ts.getTimeslot());
              Flux<Downsampling> downsamplingFlux = this.mongoOperations.find(dScanQuery, Downsampling.class);
              return downsamplingFlux.hasElements().flatMapMany(hasElements -> {
                    if (hasElements) {
                      log.info("Got hashes...");
                      return downsamplingFlux.map(d2 -> buildPending(ts.getTimeslot(), d2.getSetHash()));
                    } else {
                      log.info("Empty hashes removing timeslot...");
                      Query tsQuery = timeslotQuery(partition, group, ts.getTimeslot());
                      return this.mongoOperations.remove(tsQuery, Timeslot.class).flatMapMany(o -> Flux.empty());
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
    Query query = downsamplingQuery(partition, group, timeslot, setHash);
    return this.mongoOperations.remove(query, Downsampling.class)
            .name("removeHash")
            .metrics();
  }

  private Query downsamplingQuery(Integer partition, String group, Instant timeslot, String setHash) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").is(timeslot)
        .and("setHash").is(setHash));
    query.withHint("{ partition: 1, group: 1, timeslot: 1, setHash: 1 }");
    return query;
  }

  private Query downsamplingScanQuery(Integer partition, String group, Instant timeslot) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").is(timeslot));
    query.fields().include("setHash");
    query.withHint("{ partition: 1, group: 1 , timeslot: 1 }");
    query.limit((int) properties.getSetHashesProcessLimit());
    return query;
  }

  private Query timeslotQuery(Integer partition, String group, Instant timeslot) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").is(timeslot));
    query.withHint("{ partition: 1, group: 1, timeslot: 1 }");
    return query;
  }

  private Query timeslotScanQuery(Integer partition, String group) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").lt(Instant.now().minusSeconds(Duration.parse(group).getSeconds())));
    query.fields().include("timeslot");
    query.withHint("{ partition: 1, group: 1, timeslot: 1 }");
    return query;
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
