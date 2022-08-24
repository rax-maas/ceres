///*
// * Copyright 2022 Rackspace US, Inc.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// *
// */
//
//package com.rackspace.ceres.app.config;
//
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Configuration;
//
//@Configuration
//@Slf4j
//public class ElasticSearchConfig {
//
//  private final AppProperties appProperties;
//
//  @Autowired
//  public ElasticSearchConfig(AppProperties appProperties) {
//    this.appProperties = appProperties;
//  }
//
////  @Bean
////  public RestHighLevelClient client() {
////    log.info("elastic index name ", appProperties.elasticSearchIndexName);
////    log.info("elastic Search hosts ", appProperties.elasticSearchHosts);
////      ClientConfiguration clientConfiguration = ClientConfiguration.builder()
////          .connectedTo(appProperties.elasticSearchHosts)
////          .build();
////      return RestClients.create(clientConfiguration).rest();
////    }
////
////    @Bean
////    public ElasticsearchOperations elasticsearchTemplate() {
////      ElasticsearchRestTemplate elasticsearchRestTemplate = new ElasticsearchRestTemplate(client());
////      return elasticsearchRestTemplate;
////    }
//}
