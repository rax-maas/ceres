package com.rackspace.ceres.app.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Data
@Document
@CompoundIndexes(
    {@CompoundIndex(name = "downsampling_index", def = "{'partition': 1, 'group': 1, 'timeslot': 1}", unique = true)})
public class Downsampling {
    @Id
    public String id;

    public Integer partition;
    public String group;
    public String timeslot;
    public Set<String> hashes;

    public Downsampling(Integer partition, String group, String timeslot) {
        this.partition = partition;
        this.group = group;
        this.timeslot = timeslot;;
        this.hashes = new HashSet<>();
    }
}
