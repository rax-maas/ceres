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

package com.rackspace.monplat.ceres.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.config.SessionFactoryFactoryBean;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;

@Configuration
public class CassandraConfig {

  @Bean
  public ReactiveCqlTemplate cqlTemplate(ReactiveSessionFactory reactiveSessionFactory) {
    // following pattern of
    // org.springframework.boot.autoconfigure.data.cassandra.CassandraReactiveDataAutoConfiguration.reactiveCassandraTemplate
    return new ReactiveCqlTemplate(reactiveSessionFactory);
  }

  @Bean
  public CqlSessionBuilderCustomizer cqlLoggingCustomizer() {
    final Logger cqlLogger = LoggerFactory.getLogger("cql");
    return cqlSessionBuilder -> {
      cqlSessionBuilder.withRequestTracker(new RequestTracker() {
        @Override
        public void onSuccess(Request request, long latencyNanos,
                              DriverExecutionProfile executionProfile, Node node,
                              String requestLogPrefix) {
          if (request instanceof SimpleStatement) {
            cqlLogger.trace("Executed query: {}", ((SimpleStatement) request).getQuery());
          }
        }

        @Override
        public void onError(Request request, Throwable error, long latencyNanos,
                            DriverExecutionProfile executionProfile, Node node,
                            String requestLogPrefix) {
          if (request instanceof SimpleStatement) {
            cqlLogger.warn("Query failed: {}", ((SimpleStatement) request).getQuery(), error);
          }
        }

        @Override
        public void close() throws Exception {

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
