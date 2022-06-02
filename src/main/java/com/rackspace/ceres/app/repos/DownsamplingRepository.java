package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Downsampling;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface DownsamplingRepository extends MongoRepository<Downsampling, String> {
    public Downsampling findByPartitionAndGroupAndTimeslot(Integer partition, String group, String timeslot);
}
