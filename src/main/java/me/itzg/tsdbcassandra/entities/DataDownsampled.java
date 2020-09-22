package me.itzg.tsdbcassandra.entities;

import java.time.Duration;
import java.time.Instant;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("data_downsampled")
@Data
public class DataDownsampled {
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  String tenant;

  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1, value = "series_set")
  String seriesSet;

  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 2)
  Aggregator aggregator;

  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 3)
  Duration granularity;

  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 0)
  Instant ts;

  double value;
}
