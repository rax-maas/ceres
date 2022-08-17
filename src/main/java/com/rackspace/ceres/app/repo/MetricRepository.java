package com.rackspace.ceres.app.repo;

import com.rackspace.ceres.app.entities.Metric;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface MetricRepository extends ElasticsearchRepository<Metric, String> {

}
