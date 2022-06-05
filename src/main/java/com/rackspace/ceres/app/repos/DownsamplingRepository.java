package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Downsampling;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface DownsamplingRepository extends ReactiveCrudRepository<Downsampling, String> {
  @Query(value="{ 'partition' : ?0, 'group' : ?1 }", fields="{ 'timeslot' : 1}")
  Flux<Downsampling> findByPartitionAndGroupOrderByTimeslotAsc(Integer partition, String group);
}
