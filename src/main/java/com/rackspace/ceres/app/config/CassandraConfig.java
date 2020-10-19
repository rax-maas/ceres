/*
 * Copyright 2020 Rackspace US, Inc.
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
 */

package com.rackspace.ceres.app.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListenerBase;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;

@Configuration
@Slf4j
public class CassandraConfig {

  @Bean
  public ReactiveCqlTemplate cqlTemplate(ReactiveSessionFactory reactiveSessionFactory) {
    // following pattern of
    // org.springframework.boot.autoconfigure.data.cassandra.CassandraReactiveDataAutoConfiguration.reactiveCassandraTemplate
    return new ReactiveCqlTemplate(reactiveSessionFactory);
  }

  @Bean
  public CqlSessionBuilderCustomizer cqlLoggingCustomizer() {
    return cqlSessionBuilder -> {
      cqlSessionBuilder.withNodeStateListener(new NodeStateListenerBase() {
        @Override
        public void onAdd(Node node) {
          log.debug("Node={} ADD", node);
        }

        @Override
        public void onUp(Node node) {
          log.debug("Node={} UP", node);
        }

        @Override
        public void onDown(Node node) {
          log.debug("Node={} DOWN", node);
        }

        @Override
        public void onRemove(Node node) {
          log.debug("Node={} REMOVE", node);
        }
      });
    };
  }

  @Bean
  public SessionFactoryFactoryBean cassandraSessionFactory(CqlSession session,
                                                           CassandraConverter converter,
                                                           DataTablesPopulator dataTablesPopulator) {
    SessionFactoryFactoryBean sessionFactory = new SessionFactoryFactoryBean();
    sessionFactory.setSession(session);
    sessionFactory.setConverter(converter);
    sessionFactory.setKeyspacePopulator(dataTablesPopulator);
    sessionFactory.setSchemaAction(SchemaAction.CREATE_IF_NOT_EXISTS);
    return sessionFactory;
  }

}
