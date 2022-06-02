package com.rackspace.ceres.app.model;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;

import java.time.Instant;

@Data
public class Job {
    @Id
    public String id;

    public Integer partition;
    public String group;
    public String status;
    public Instant timestamp;

    public Job(Integer partition, String group, String status) {
        this.partition = partition;
        this.group = group;
        this.status = status;
        this.timestamp = Instant.now();
    }
}
