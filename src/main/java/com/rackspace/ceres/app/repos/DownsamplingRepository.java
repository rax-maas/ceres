package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Downsampling;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface DownsamplingRepository extends ReactiveCrudRepository<Downsampling, String> {
    Mono<Downsampling> findByPartitionAndGroupAndTimeslot(Integer partition, String group, String timeslot);
}
