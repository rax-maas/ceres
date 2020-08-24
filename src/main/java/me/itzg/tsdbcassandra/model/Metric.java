package me.itzg.tsdbcassandra.model;

import java.time.Instant;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class Metric {
  @NotNull
  Instant ts;
  @NotBlank
  String tenant;
  @NotBlank
  String metricName;
  @NotEmpty
  Map<String,String> tags;
  double value;
}
