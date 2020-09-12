package me.itzg.tsdbcassandra.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.ReactiveSessionFactory;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;

@Configuration
public class CassandraConfig {

  @Bean
  public ReactiveCqlTemplate cqlTemplate(ReactiveSessionFactory cqlSessionFactory) {
    return new ReactiveCqlTemplate(cqlSessionFactory);
  }
}
