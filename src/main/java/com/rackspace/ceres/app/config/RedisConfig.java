package com.rackspace.ceres.app.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.script.RedisScript;

@Configuration
public class RedisConfig {

  @Bean
  public RedisScript<Boolean> redisScript() {
    Resource scriptSource = new ClassPathResource("check-partition-job.lua");
    return RedisScript.of(scriptSource, Boolean.class);
  }
}
