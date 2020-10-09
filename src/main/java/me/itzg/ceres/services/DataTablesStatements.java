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

package me.itzg.ceres.services;

import java.time.Duration;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Component;

/**
 * Provides a caching layer for insert and query statements that execute against the configuration-driven
 * data tables schema.
 */
@Component
public class DataTablesStatements {

  public static final String TENANT = "tenant";
  public static final String SERIES_SET = "series_set";
  public static final String AGGREGATOR = "aggregator";
  public static final String TIMESTAMP = "ts";
  public static final String VALUE = "value";

  public String tableNameRaw() {
    return "data_raw";
  }

  public String tableNameDownsampled(Duration granularity) {
    return "data_" + granularity.toString().toLowerCase();
  }

  /**
   * @return an INSERT CQL statement with placeholders
   * tenant, seriesSet, timestamp, value
   */
  @Cacheable("insertRaw")
  public String insertRaw() {
    return "INSERT INTO " + tableNameRaw()
        + " (" + String.join(",", TENANT, SERIES_SET, TIMESTAMP, VALUE) + ")"
        + " VALUES (?, ?, ?, ?)";
  }

  /**
   * @return an INSERT CQL statement with placeholders
   * tenant, seriesSet, aggregator, timestamp, value
   */
  @Cacheable("insertDownsampled")
  public String insertDownsampled(Duration granularity) {
    return "INSERT INTO " + tableNameDownsampled(granularity)
        + " (" + String.join(",", TENANT, SERIES_SET, AGGREGATOR, TIMESTAMP, VALUE) + ")"
        + " VALUES (?, ?, ?, ?, ?)";
  }

  /**
   * @return a SELECT CQL statement with placeholders
   * tenant, seriesSet, starting timestamp, ending timestamp
   * and returns timestamp, value
   */
  @Cacheable("queryRaw")
  public String queryRaw() {
    return "SELECT " + String.join(",", TIMESTAMP, VALUE)
        + " FROM " + tableNameRaw()
        + " WHERE"
        + "  " + TENANT + " = ?"
        + "  AND " + SERIES_SET + " = ?"
        + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?";
  }

  /**
   * @return a SELECT CQL statement with placeholders
   * tenant, seriesSet, aggregator, starting timestamp, ending timestamp
   * and returns timestamp, value
   */
  @Cacheable("queryDownsampled")
  public String queryDownsampled(Duration granularity) {
    return "SELECT " + String.join(",", TIMESTAMP, VALUE)
        + " FROM " + tableNameDownsampled(granularity)
        + " WHERE"
        + "  " + TENANT + " = ?"
        + "  AND " + SERIES_SET + " = ?"
        + "  AND " + AGGREGATOR + " = ?"
        + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?";
  }
}
