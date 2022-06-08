package com.rackspace.ceres.app.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@Table("pending_timeslots")
public class Pending {
    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    private Integer partition;
    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1)
    private String group;
    @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 2)
    private long timeslot;
}
