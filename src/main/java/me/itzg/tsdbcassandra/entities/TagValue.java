package me.itzg.tsdbcassandra.entities;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@Table("tag_values")
public class TagValue {

  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  String tenant;

  @PrimaryKeyColumn(value = "metric_name", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  String metricName;

  @PrimaryKeyColumn(value = "tag_key", type = PrimaryKeyType.CLUSTERED, ordinal = 2)
  String tagKey;

  @PrimaryKeyColumn(value = "tag_value", type = PrimaryKeyType.CLUSTERED, ordinal = 3)
  String tagValue;
}
