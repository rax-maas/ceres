package com.rackspace.ceres.app.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.util.HashSet;
import java.util.Set;

@Data
@AllArgsConstructor
@Table("downsampling_hashes")
public class Downsampling {
    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    private long timeslot;
    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 1)
    private String group;
    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 2)
    private int partition;
    @PrimaryKeyColumn(type = PrimaryKeyType.CLUSTERED, ordinal = 3)
    private String hash;
    private boolean completed;
}
