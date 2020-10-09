/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
