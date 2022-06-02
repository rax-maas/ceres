package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Job;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface JobRepository extends MongoRepository<Job, String> {
}

