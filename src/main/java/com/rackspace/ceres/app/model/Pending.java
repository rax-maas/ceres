package com.rackspace.ceres.app.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.Set;

@Data
@Document
@CompoundIndexes({@CompoundIndex(name = "pending_index", def = "{'partition': 1, 'group': 1}")})
public class Pending {
    @Id
    public String id;

    public Integer partition;
    public String group;
    public Set<String> timeslots;

    public Pending(Integer partition, String group) {
        this.partition = partition;
        this.group = group;
        this.timeslots = new HashSet<>();
    }
}
