package com.rackspace.ceres.app.repos;

import com.rackspace.ceres.app.model.Job;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobRepository extends ReactiveCrudRepository<Job, String> {
}

