package com.rackspace.ceres.app.model;

import java.util.List;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import lombok.Data;

@Data
public class TsdbQueryRequestData {
  @NotNull
  String start;

  String end;

  @NotEmpty
  List<TsdbQueryRequest> queries;
}
