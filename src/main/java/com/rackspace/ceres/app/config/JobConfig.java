package com.rackspace.ceres.app.config;

import com.rackspace.ceres.app.model.Job;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Configuration
public class JobConfig {
    private final DownsampleProperties properties;

    public JobConfig(DownsampleProperties properties) {
        this.properties = properties;
    }

    @Bean
    public List<Job> jobList() {
        List<Job> jobList = new ArrayList<>();
        properties.getGranularities().stream().map(granularity -> granularity.getPartitionWidth().toString())
                .forEach(group -> IntStream.rangeClosed(0, properties.getPartitions() - 1)
                        .forEach(partition -> jobList.add(new Job(partition, group, "free"))));
        return jobList;
    }
}