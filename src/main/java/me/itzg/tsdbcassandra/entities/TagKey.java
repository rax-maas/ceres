package me.itzg.tsdbcassandra.entities;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("tag_keys")
@Data
public class TagKey {
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  String tenant;

  @PrimaryKeyColumn(value = "metric_name", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  String metricName;

  @PrimaryKeyColumn(value = "tag_key", type = PrimaryKeyType.CLUSTERED, ordinal = 2)
  String tagKey;
}
