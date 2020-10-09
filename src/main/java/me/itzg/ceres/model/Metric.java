package me.itzg.ceres.model;

import java.time.Instant;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class Metric {
  @NotNull
  Instant timestamp;
  @NotBlank
  String metric;
  @NotEmpty
  Map<String,String> tags;
  @NotNull
  Number value;
}
