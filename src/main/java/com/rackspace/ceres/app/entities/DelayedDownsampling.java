package com.rackspace.ceres.app.entities;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@AllArgsConstructor
@Table("delayed_hashes")
public class DelayedDownsampling {
  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  private int partition;
  @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 2)
  private String hash;
}
