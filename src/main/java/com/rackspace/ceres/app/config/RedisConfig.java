package com.rackspace.ceres.app.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

@Configuration
public class RedisConfig {
  @Bean
  public RedisScript<List> redisGetTimeSlots() {
    Resource scriptSource = new ClassPathResource("get-time-slots.lua");
    return RedisScript.of(scriptSource, List.class);
  }

  @Bean
  public RedisScript<String> redisGetJob() {
    Resource scriptSource = new ClassPathResource("check-job.lua");
    return RedisScript.of(scriptSource, String.class);
  }

  @Bean
  public RedisScript<String> redisCheckOldTimeSlots() {
    Resource scriptSource = new ClassPathResource("check-old-timeslots.lua");
    return RedisScript.of(scriptSource, String.class);
  }
}
