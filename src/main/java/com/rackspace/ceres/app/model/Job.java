package com.rackspace.ceres.app.model;
import lombok.Data;

@Data
public class Job {
    private Integer partition;
    private String group;
    private String status;

    public Job(Integer partition, String group, String status) {
        this.partition = partition;
        this.group = group;
        this.status = status;
    }
}
