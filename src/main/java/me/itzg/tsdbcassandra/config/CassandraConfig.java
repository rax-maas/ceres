package me.itzg.tsdbcassandra.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
        public void onSuccess(@NotNull Request request, long latencyNanos,
                              @NotNull DriverExecutionProfile executionProfile, @NotNull Node node,
                              @NotNull String requestLogPrefix) {
          if (request instanceof SimpleStatement) {
            cqlLogger.trace("Executed query: {}", ((SimpleStatement) request).getQuery());
          }
        }

        @Override
        public void onError(@NotNull Request request, @NotNull Throwable error, long latencyNanos,
                            @NotNull DriverExecutionProfile executionProfile, @Nullable Node node,
                            @NotNull String requestLogPrefix) {
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
