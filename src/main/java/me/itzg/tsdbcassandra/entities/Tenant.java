package me.itzg.tsdbcassandra.entities;

import lombok.Data;
import org.springframework.data.cassandra.core.mapping.PrimaryKey;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("tenants")
@Data
public class Tenant {

  /**
   * The ID of the tenant, but named "tenant" to line up with other table schemas.
   */
  @PrimaryKey
  String tenant;
}
