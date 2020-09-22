package me.itzg.tsdbcassandra.services;

import java.time.Instant;
import org.springframework.stereotype.Component;

@Component
public class TimestampProvider {

  public Instant now() {
    return Instant.now();
  }
}
