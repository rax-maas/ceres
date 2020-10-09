package me.itzg.ceres.model;

import java.time.Instant;
import lombok.Data;

@Data
public class PendingDownsampleSet {
  int partition;

  Instant timeSlot;

  String tenant;

  String seriesSet;
}
