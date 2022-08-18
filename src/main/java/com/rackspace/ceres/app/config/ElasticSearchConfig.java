/*
 * Copyright 2022 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.rackspace.ceres.app.config;

import com.rackspace.ceres.app.entities.Metric;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
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

  private final AppProperties appProperties;

  @Autowired
  public ElasticSearchConfig(AppProperties appProperties) {
    this.appProperties = appProperties;
  }

  @Bean
  public RestHighLevelClient client() {
      ClientConfiguration clientConfiguration = ClientConfiguration.builder()
          .connectedTo(appProperties.elasticSearchHosts)
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
