package me.itzg.ceres.web;

import java.util.List;
import org.springframework.util.MultiValueMap;

public class ParamUtils {

  /**
   * The OpenTSDB APIs use a convention where a query parameter that is present, but doesn't have
   * any value indicates that it is "enabled". Given a {@link MultiValueMap} obtained using
   * {@link org.springframework.web.bind.annotation.RequestParam}, this method will a parameter
   * that is present but unset to indicate a true value. If the parameter has a value, then it is
   * parsed as a boolean.
   * @param params all of the request's query parameters
   * @param key the query parameter to check
   * @return true if the query parameter was present and unset or a true value
   */
  static boolean paramPresentOrTrue(MultiValueMap<String, String> params, String key) {
    final List<String> values = params.get(key);
    if (values == null || values.isEmpty()) {
      return false;
    } else {
      final String maybeValue = values.get(0);
      if (maybeValue == null) {
        return true;
      } else {
        return Boolean.parseBoolean(maybeValue);
      }
    }
  }
}
