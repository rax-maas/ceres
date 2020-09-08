package me.itzg.tsdbcassandra.downsample;

import java.time.Instant;
import lombok.Data;

@Data
public abstract class ValueSet {
  Instant timestamp;
}
