package me.itzg.ceres.config;

import org.springframework.boot.task.TaskSchedulerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;

@Configuration
public class SchedulerConfig {

  @Bean
  public TaskScheduler downsampleTaskScheduler() {
    return new TaskSchedulerBuilder()
        .poolSize(Runtime.getRuntime().availableProcessors())
        .threadNamePrefix("downsample")
        .build();
  }
}
