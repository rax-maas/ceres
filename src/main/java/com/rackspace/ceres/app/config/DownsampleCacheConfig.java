package com.rackspace.ceres.app.config;

import com.rackspace.ceres.app.downsample.ValueSet;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Flux;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
public class DownsampleCacheConfig {
  Map<String, Flux<? extends ValueSet>> dataPoints = new ConcurrentHashMap<>();

  @Bean
  public Map<String, Flux<? extends ValueSet>> dataPoints() {
    return dataPoints;
  }
}
