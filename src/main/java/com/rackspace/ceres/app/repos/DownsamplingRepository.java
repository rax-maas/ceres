package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Downsampling;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public interface DownsamplingRepository extends ReactiveCrudRepository<Downsampling, String> {
    Mono<Downsampling> deleteByPartitionAndGroupAndTimeslotAndSetHash(
        Integer partition, String group, Instant timeslot, String setHash);
}
