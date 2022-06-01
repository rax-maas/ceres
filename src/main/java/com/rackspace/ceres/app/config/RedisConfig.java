package com.rackspace.ceres.app.config;


import io.lettuce.core.ReadFrom;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.time.Duration;

@ConditionalOnProperty(prefix = "spring", name = "redis.cluster.nodes")
@Configuration
public class RedisConfig {

  @Autowired
  private RedisProperties redisProperties;

  @Value("${redis.maxRedirects:3}")
  private int maxRedirects;

  @Value("${redis.refreshTime:5}")
  private int refreshTime;

  @Bean
  public LettuceConnectionFactory redisConnectionFactory() {
    RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration(redisProperties.getCluster().getNodes());
    redisClusterConfiguration.setMaxRedirects(maxRedirects);
    redisClusterConfiguration.setPassword(redisProperties.getPassword());
    // Support adaptive cluster topology refresh and static refresh source
    //In case a master node failure this configuration manage the whole topology for other nodes.
    ClusterTopologyRefreshOptions clusterTopologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
            .enablePeriodicRefresh()
            .enableAllAdaptiveRefreshTriggers()
            .refreshPeriod(Duration.ofSeconds(refreshTime))
            .build();

    ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
            .topologyRefreshOptions(clusterTopologyRefreshOptions).build();
    LettuceClientConfiguration lettuceClientConfiguration = LettuceClientConfiguration.builder()
            .readFrom(ReadFrom.ANY)
            .clientOptions(clusterClientOptions).build();
    return new LettuceConnectionFactory(redisClusterConfiguration, lettuceClientConfiguration);
  }
}
