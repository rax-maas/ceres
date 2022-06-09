package com.rackspace.ceres.app.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

@Data
@Document
@CompoundIndexes(
    {
        @CompoundIndex(name = "downsampling_index", def = "{'partition': 1, 'group': 1, 'timeslot': 1, 'setHash': 1}", unique = true),
        @CompoundIndex(name = "downsampling_index_pg", def = "{'partition': 1, 'group': 1}"),
        @CompoundIndex(name = "downsampling_index_pgt", def = "{'partition': 1, 'group': 1, 'timeslot': 1}")
    }
)
public class Downsampling {
    @Id
    public String id;

    public Integer partition;
    public String group;
    @Indexed
    public Instant timeslot;
    public String setHash;

    public Downsampling(Integer partition, String group, Instant timeslot, String setHash) {
        this.partition = partition;
        this.group = group;
        this.timeslot = timeslot;
        this.setHash = setHash;
    }
}
