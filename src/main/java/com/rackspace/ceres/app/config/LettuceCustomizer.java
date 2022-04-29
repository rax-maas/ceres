package com.rackspace.ceres.app.config;

import io.lettuce.core.ClientOptions;
import org.springframework.boot.autoconfigure.data.redis.LettuceClientConfigurationBuilderCustomizer;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;

@Configuration
public class LettuceCustomizer implements LettuceClientConfigurationBuilderCustomizer {

    @Override
    public void customize(LettuceClientConfiguration.LettuceClientConfigurationBuilder clientConfigurationBuilder) {
        clientConfigurationBuilder.clientOptions(ClientOptions.builder().publishOnScheduler(true).build());
    }
}
