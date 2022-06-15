package com.rackspace.ceres.app.model;

import lombok.Data;

@Data
public class TimeslotCacheKey {
  final Integer partition;
  final String group;
  final Long timeslot;
}
