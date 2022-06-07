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
import com.rackspace.ceres.app.utils.DateTimeUtils;
import com.rackspace.ceres.app.utils.WebClientUtils;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Comparator;

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
          final String timeslot = Long.toString(normalizedTimeSlot.getEpochSecond());
          final String pendingValue = encodingPendingValue(timeslot, tenant, seriesSetHash);
          return saveDownsampling(partition, width, normalizedTimeSlot, pendingValue);
        }
    );
  }

  private Mono<?> saveDownsampling(Integer partition, String group, Instant timeslot, String value) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition).and("group").is(group).and("timeslot").is(timeslot));
    query.fields().exclude("hashes");
    Update update = new Update();
    update.addToSet("hashes", value);
    FindAndModifyOptions options = new FindAndModifyOptions();
    options.returnNew(true).upsert(true);

    return this.mongoOperations.findAndModify(query, update, options, Downsampling.class)
        .flatMap(downsampling -> Mono.empty());
  }

  public Flux<PendingDownsampleSet> getDownsampleSets(int partition, String group) {
    log.trace("getDownsampleSets {} {}", partition, group);
    long partitionWidth = Duration.parse(group).getSeconds();
    String queryString =
        String.format("{ \"partition\" : %d, \"group\" : \"%s\" }", partition, group);
    String fields = String.format("{ \"hashes\": { $slice: %d } }", properties.getSetHashesProcessLimit());
    BasicQuery query = new BasicQuery(queryString, fields);
    // TODO: Move the is due check and sort into mongodb query
//    query.addCriteria(Criteria.where("timeslot").gt(Instant.now().minusSeconds(partitionWidth)));
//    query.with(Sort.by(Sort.Direction.ASC, "timeslot"));

    return this.mongoOperations.find(query, Downsampling.class)
        .filter(d -> isDueTimeslot(d.getTimeslot(), partitionWidth, Instant.now()))
        .sort(Comparator.comparing(Downsampling::getTimeslot))
        .take(1)
        .flatMap(
            downsampling -> {
                log.info("Hashes count: {} partition: {}, group: {}, timeslot: {}",
                    downsampling.getHashes().size(), partition, group,
                    downsampling.getTimeslot().atZone(ZoneId.systemDefault()).toLocalDateTime());
                return Flux.fromIterable(downsampling.getHashes());
            })
        .map(DownsampleTrackingService::buildPending);
  }

  public Mono<?> complete(PendingDownsampleSet entry, Integer partition, String group) {
    final String timeslot = Long.toString(entry.getTimeSlot().getEpochSecond());
    final String value = encodingPendingValue(timeslot, entry.getTenant(), entry.getSeriesSetHash());
    return removeHash(partition, group, entry.getTimeSlot(), value);
  }

  private Mono<?> removeHash(Integer partition, String group, Instant timeslot, String value) {
    Query query = new Query();
    query.addCriteria(Criteria.where("partition").is(partition).and("group").is(group).and("timeslot").is(timeslot));
    query.fields().exclude("hashes");
    Update update = new Update();
    update.pull("hashes", value);
    FindAndModifyOptions options = new FindAndModifyOptions();
    options.returnNew(true);

    return this.mongoOperations.findAndModify(query, update, options, Downsampling.class)
        .flatMap(downsampling -> cleanEmptyHashes(partition, group, timeslot));
  }

  private static String encodingPendingValue(String timeslot, String tenant, String seriesSet) {
    return timeslot + DELIM + tenant + DELIM + seriesSet;
  }

  private static PendingDownsampleSet buildPending(String pendingValue) {
    log.trace("buildPending {}", pendingValue);
    String[] values = pendingValue.split("\\|");

    return new PendingDownsampleSet()
        .setTenant(values[1])
        .setSeriesSetHash(values[2])
        .setTimeSlot(Instant.ofEpochSecond(Long.parseLong(values[0])));
  }

  public Mono<?> cleanEmptyHashes(int partition, String group, Instant timeslot) {
    Query query = new Query();
    query.addCriteria(Criteria
        .where("partition").is(partition)
        .and("group").is(group)
        .and("timeslot").is(timeslot)
        .and("hashes").size(0));
    return this.mongoOperations.findAndRemove(query, Downsampling.class)
        .flatMap(downsampling -> {
          log.info("Removed empty hashes: {} {} {}",
              timeslot.atZone(ZoneId.systemDefault()).toLocalDateTime(),
              downsampling.getPartition(), downsampling.getGroup());
          return Mono.empty();
        });
  }

  private boolean isDueTimeslot(Instant timeslot, long partitionWidth, Instant now) {
    boolean isDue = timeslot.getEpochSecond() + partitionWidth < now.getEpochSecond();
    log.trace("Timeslot is due: {}, {}, {}", isDue, timeslot.atZone(ZoneId.systemDefault()).toLocalDateTime(), partitionWidth);
    return isDue;
  }
}
