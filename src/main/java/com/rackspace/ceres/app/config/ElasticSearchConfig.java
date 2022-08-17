package com.rackspace.ceres.app.config;

import com.rackspace.ceres.app.entities.Metric;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

@Configuration
@EnableElasticsearchRepositories(basePackages = "com.rackspace.ceres.app.repo")
@ComponentScan(basePackages = { "com.rackspace.ceres.app.services" })
public class ElasticSearchConfig {

  @Bean
  public RestHighLevelClient client() {
      ClientConfiguration clientConfiguration = ClientConfiguration.builder()
          .connectedTo("localhost:9200")
          .build();
      return RestClients.create(clientConfiguration).rest();
    }

    @Bean
    public ElasticsearchOperations elasticsearchTemplate() {
      ElasticsearchRestTemplate elasticsearchRestTemplate = new ElasticsearchRestTemplate(client());
      elasticsearchRestTemplate.indexOps(Metric.class);
      return elasticsearchRestTemplate;
    }
}
