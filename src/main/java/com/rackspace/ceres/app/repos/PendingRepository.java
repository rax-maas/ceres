package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Pending;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface PendingRepository extends MongoRepository<Pending, String> {
    public Pending findByPartitionAndGroup(Integer partition, String group);
}
