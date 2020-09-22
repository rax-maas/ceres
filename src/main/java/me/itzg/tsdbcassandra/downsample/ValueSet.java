package me.itzg.tsdbcassandra.downsample;

import java.time.Instant;
import lombok.Data;

/**
 * Holds a timestamped set of one or more related values.
 */
@Data
public abstract class ValueSet {
  Instant timestamp;
}
