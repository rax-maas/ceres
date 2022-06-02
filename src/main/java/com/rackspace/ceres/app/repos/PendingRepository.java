package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Pending;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface PendingRepository extends ReactiveCrudRepository<Pending, String> {
    Mono<Pending> findByPartitionAndGroup(Integer partition, String group);
}
