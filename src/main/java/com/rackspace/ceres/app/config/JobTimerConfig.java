package com.rackspace.ceres.app.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

@Configuration
public class JobTimerConfig {
    private final DownsampleProperties properties;

    public JobTimerConfig(DownsampleProperties properties) {
        this.properties = properties;
    }

    @Bean
    public List<Instant> jobTimers() {
        List<Instant> jobTimers = new ArrayList<>();
        properties.getGranularities().stream().map(granularity -> granularity.getPartitionWidth().toString())
                .forEach(group -> IntStream.rangeClosed(0, properties.getPartitions() - 1)
                        .forEach(partition -> jobTimers.add(Instant.now())));
        return jobTimers;
    }
}
