package me.itzg.tsdbcassandra.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.cassandra.CassandraProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.ReactiveSession;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;

@Configuration
public class CassandraConfig {

  private final CassandraProperties cassandraProperties;

  @Autowired
  public CassandraConfig(CassandraProperties cassandraProperties) {
    this.cassandraProperties = cassandraProperties;
  }

  @Bean
  public ReactiveCqlTemplate cqlTemplate(ReactiveSession cqlSession) {
    return new ReactiveCqlTemplate(cqlSession);
  }
}
