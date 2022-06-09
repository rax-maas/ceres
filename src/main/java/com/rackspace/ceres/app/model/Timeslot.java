package com.rackspace.ceres.app.model;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Document
@CompoundIndexes(
    {
        @CompoundIndex(name = "timeslot_index", def = "{'partition': 1, 'group': 1, 'timeslot': 1}", unique = true),
        @CompoundIndex(name = "timeslot_index_pg", def = "{'partition': 1, 'group': 1}")
    }
)
public class Timeslot {
  @Id
  public String id;

  public Integer partition;
  public String group;
  @Indexed
  public Instant timeslot;

  public Timeslot(Integer partition, String group, Instant timeslot) {
    this.partition = partition;
    this.group = group;
    this.timeslot = timeslot;
  }
}
