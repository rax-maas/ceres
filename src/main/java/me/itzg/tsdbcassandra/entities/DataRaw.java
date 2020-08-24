package me.itzg.tsdbcassandra.entities;

import java.time.Instant;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("data_raw")
@Data
public class DataRaw {
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED)
  String tenant;
  @PrimaryKeyColumn(value = "series_set", type = PrimaryKeyType.PARTITIONED)
  String seriesSet;
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED)
  Instant ts;
  double value;
}
