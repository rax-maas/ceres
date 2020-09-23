package me.itzg.tsdbcassandra.model;

import java.time.Instant;
import lombok.Data;

@Data
public class PendingDownsampleSet {
  Instant timeSlot;

  String tenant;

  String seriesSet;
}
