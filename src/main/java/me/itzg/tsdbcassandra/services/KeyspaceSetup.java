package me.itzg.tsdbcassandra.services;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.stereotype.Service;

@Service
public class KeyspaceSetup {

  private final CassandraTemplate cassandraTemplate;

  @Autowired
  public KeyspaceSetup(CassandraTemplate cassandraTemplate) {
    this.cassandraTemplate = cassandraTemplate;
  }

  @EventListener(ApplicationStartedEvent.class)
  public void onApplicationStarted() {

  }
}
