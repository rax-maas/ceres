package me.itzg.ceres.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.List;
import lombok.Data;

@Data
public class PutResponse {

  long success;
  long failed;
  @JsonInclude(Include.NON_NULL)
  List<Error> errors;

  @Data
  public static class Error {
      Metric datapoint;
      String error;
  }
}
