package me.itzg.tsdbcassandra.entities;

import java.time.Instant;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("pending_downsample_sets")
@Data
public class PendingDownsampleSet {
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  int partition;
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  String tenant;
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 0, value = "time_slot")
  Instant timeSlot;
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 1, value = "series_set")
  String seriesSet;

  Instant lastTouch;
}
