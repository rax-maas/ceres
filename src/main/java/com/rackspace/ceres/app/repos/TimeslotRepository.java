package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Timeslot;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public interface TimeslotRepository extends ReactiveCrudRepository<Timeslot, String> {
  Mono<Timeslot> deleteByPartitionAndGroupAndTimeslot(Integer partition, String group, Instant timeslot);
}
