package me.itzg.tsdbcassandra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;

@Configuration
public class CassandraConfig {

  @Bean
  public ReactiveCqlTemplate cqlTemplate(ReactiveSessionFactory reactiveSessionFactory) {
    // following pattern of
    // org.springframework.boot.autoconfigure.data.cassandra.CassandraReactiveDataAutoConfiguration.reactiveCassandraTemplate
    return new ReactiveCqlTemplate(reactiveSessionFactory);
  }
}
