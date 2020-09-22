package me.itzg.tsdbcassandra.entities;

import java.util.Set;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("metric_names")
@Data
public class MetricName {
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  String tenant;

  @PrimaryKeyColumn(value = "metric_name",
      /*
      Differs from other tables in order to allow efficient query of
      SELECT metric_name FROM metric_names WHERE tenant = ?
       */
      type = PrimaryKeyType.CLUSTERED,
      ordinal = 1)
  String metricName;

  Set<Aggregator> aggregators;
}
