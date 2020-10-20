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

package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provides a consolidated declaration of insert and query statements that execute against the configuration-driven
 * data tables schema.
 */
@Component
public class DataTablesStatements {

  public static final String TENANT = "tenant";
  public static final String SERIES_SET_HASH = "series_set_hash";
  public static final String AGGREGATOR = "aggregator";
  public static final String TIMESTAMP = "ts";
  public static final String VALUE = "value";
  public static final String TABLE_NAME_RAW = "data_raw";
  private static final String TABLE_PREFIX_DOWNSAMPLED = "data_";

  /**
   * INSERT CQL statement with placeholders
   * tenant, seriesSetHash, timestamp, value
   */
  public static final String INSERT_RAW = "INSERT INTO " + TABLE_NAME_RAW
      + " (" + String.join(",", TENANT, SERIES_SET_HASH, TIMESTAMP, VALUE) + ")"
      + " VALUES (?, ?, ?, ?)";

  /**
   * A SELECT CQL statement with placeholders
   * tenant, seriesSetHash, starting timestamp, ending timestamp
   * and returns timestamp, value
   */
  public static final String QUERY_RAW = "SELECT " + String.join(",", TIMESTAMP, VALUE)
      + " FROM " + TABLE_NAME_RAW
      + " WHERE"
      + "  " + TENANT + " = ?"
      + "  AND " + SERIES_SET_HASH + " = ?"
      + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?";

  private final Map<Duration, String> downsampleInserts = new HashMap<>();
  private final Map<Duration, String> downsampleQueries = new HashMap<>();

  @Autowired
  public DataTablesStatements(DownsampleProperties downsampleProperties) {
    buildDownsampleStatements(downsampleProperties);
  }

  private void buildDownsampleStatements(DownsampleProperties downsampleProperties) {
    if (downsampleProperties.getGranularities() == null) {
      return;
    }

    downsampleProperties.getGranularities().stream()
        .map(Granularity::getWidth)
        .forEach(granularity -> {

          downsampleInserts.put(granularity,
            "INSERT INTO " + tableNameDownsampled(granularity)
                + " (" + String.join(",", TENANT, SERIES_SET_HASH, AGGREGATOR, TIMESTAMP, VALUE) + ")"
                + " VALUES (?, ?, ?, ?, ?)"
          );

          downsampleQueries.put(granularity,
              "SELECT " + String.join(",", TIMESTAMP, VALUE)
                  + " FROM " + tableNameDownsampled(granularity)
                  + " WHERE"
                  + "  " + TENANT + " = ?"
                  + "  AND " + SERIES_SET_HASH + " = ?"
                  + "  AND " + AGGREGATOR + " = ?"
                  + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?"
              );
        });
  }

  public String tableNameDownsampled(Duration granularity) {
    return TABLE_PREFIX_DOWNSAMPLED + granularity.toString().toLowerCase();
  }

  /**
   * @return an INSERT CQL statement with placeholders
   * tenant, seriesSetHash, aggregator, timestamp, value
   */
  public String downsampleInsert(Duration granularity) {
    return downsampleInserts.get(granularity);
  }

  /**
   * @return a SELECT CQL statement with placeholders
   * tenant, seriesSetHash, aggregator, starting timestamp, ending timestamp
   * and returns timestamp, value
   */
  public String queryDownsampled(Duration granularity) {
    return downsampleQueries.get(granularity);
  }
}
