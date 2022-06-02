package com.rackspace.ceres.app.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.HashSet;
import java.util.Set;

@Data
@Document
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
        this.timeslot = timeslot;
        this.hashes = new HashSet<>();
    }
}
