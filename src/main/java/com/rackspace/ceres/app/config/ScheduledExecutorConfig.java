package com.rackspace.ceres.app.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

@Configuration
public class ScheduledExecutorConfig {
    private final DownsampleProperties downsampleProperties;

    @Autowired
    public ScheduledExecutorConfig(DownsampleProperties downsampleProperties) {
        this.downsampleProperties = downsampleProperties;
    }

    @Bean
    public ScheduledExecutorService scheduledExecutorService() {
        return Executors.newScheduledThreadPool(downsampleProperties.getProcessingThreads());
    }
}
