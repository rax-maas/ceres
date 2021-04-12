package com.rackspace.ceres.app.model;

import java.time.Instant;
import java.util.List;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class TsdbQueryRequestData {
  @NotNull
  Instant start;

  Instant end;

  @NotEmpty
  List<TsdbQueryRequest> queries;
}
