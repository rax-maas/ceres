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

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provides a consolidated declaration of insert and query statements that execute against the
 * configuration-driven data tables schema.
 */
@Component
@Slf4j
public class DataTablesStatements {

  public static final String TENANT = "tenant";
  public static final String TIME_PARTITION_SLOT = "time_slot";
  public static final String SERIES_SET_HASH = "series_set_hash";
  public static final String AGGREGATOR = "aggregator";
  public static final String TIMESTAMP = "ts";
  public static final String VALUE = "value";

  private static final String TABLE_PREFIX = "data";
  private static final String RAW = "raw";

  private String rawInsert;
  private String rawQuery;
  private String rawDelete;
  private String rawGetSeriesSetHashQuery;
  private String rawDeleteWithSeriesSetHash;

  private final Map<Duration, String> downsampleInserts = new HashMap<>();
  private final Map<Duration, String> downsampleQueries = new HashMap<>();
  private final Map<Duration, String> downsampleDeletes = new HashMap<>();
  private final Map<Duration, String> downsampleDeletesWithSeriesSetHash = new HashMap<>();

  @Autowired
  public DataTablesStatements(AppProperties appProperties,
      DownsampleProperties downsampleProperties) {
    buildRawStatements(appProperties);
    buildDownsampleStatements(downsampleProperties);
  }

  public String tableNameRaw(Duration partitionWidth) {
    return String.join("_",
        TABLE_PREFIX, RAW, partitionTableNameSuffix(partitionWidth)
    );
  }

  String partitionTableNameSuffix(Duration width) {
    return "p_" + width.toString().toLowerCase();
  }

  private void buildRawStatements(AppProperties appProperties) {
    rawInsert = "INSERT INTO " + tableNameRaw(appProperties.getRawPartitionWidth())
        + " (" +
        String.join(",", TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, TIMESTAMP, VALUE)
        + ")"
        + " VALUES (?, ?, ?, ?, ?)";
    rawQuery = "SELECT " + String.join(",", TIMESTAMP, VALUE)
        + " FROM " + tableNameRaw(appProperties.getRawPartitionWidth())
        + " WHERE " + TENANT + " = ?"
        + "  AND " + TIME_PARTITION_SLOT + " = ?"
        + "  AND " + SERIES_SET_HASH + " = ?"
        + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?";
    rawDelete = "DELETE FROM " + tableNameRaw(appProperties.getRawPartitionWidth())
        + " WHERE " + TENANT + " = ?"
        + "  AND " + TIME_PARTITION_SLOT + " = ?";
    rawDeleteWithSeriesSetHash = "DELETE FROM " + tableNameRaw(appProperties.getRawPartitionWidth())
        + " WHERE " + TENANT + " = ?"
        + "  AND " + TIME_PARTITION_SLOT + " = ?"
        + " AND series_set_hash = ?";
    rawGetSeriesSetHashQuery =
        "SELECT series_set_hash FROM " + tableNameRaw(appProperties.getRawPartitionWidth())
            + " WHERE " + TENANT + " = ?"
            + "  AND " + TIME_PARTITION_SLOT + " = ?";
  }

  private void buildDownsampleStatements(DownsampleProperties downsampleProperties) {
    if (downsampleProperties.getGranularities() == null) {
      return;
    }

    downsampleProperties.getGranularities()
        .forEach(granularity -> {

          downsampleInserts.put(granularity.getWidth(),
              "INSERT INTO " + tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth())
                  + " ("
                  + String.join(",",
                  TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, AGGREGATOR, TIMESTAMP, VALUE)
                  + ")"
                  + " VALUES (?, ?, ?, ?, ?, ?)"
          );

          downsampleQueries.put(granularity.getWidth(),
              "SELECT " + String.join(",", TIMESTAMP, VALUE)
                  + " FROM " + tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth())
                  + " WHERE " + TENANT + " = ?"
                  + "  AND " + TIME_PARTITION_SLOT + " = ?"
                  + "  AND " + SERIES_SET_HASH + " = ?"
                  + "  AND " + AGGREGATOR + " = ?"
                  + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?"
          );

          downsampleDeletes.put(granularity.getWidth(),
              "DELETE FROM " + tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth())
                  + " WHERE " + TENANT + " = ?"
                  + "  AND " + TIME_PARTITION_SLOT + " = ?"
          );

          downsampleDeletesWithSeriesSetHash.put(granularity.getWidth(),
              "DELETE FROM " + tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth())
                  + " WHERE " + TENANT + " = ?"
                  + "  AND " + TIME_PARTITION_SLOT + " = ?"
                  + " AND series_set_hash = ?"
          );
        });
  }

  public String tableNameDownsampled(Duration granularity, Duration partitionWidth) {
    return String.join("_",
        TABLE_PREFIX,
        granularity.toString().toLowerCase(),
        partitionTableNameSuffix(partitionWidth)
    );
  }

  /**
   * @return INSERT CQL statement with placeholders tenant, timeSlot, seriesSetHash, timestamp,
   * value
   */
  public String rawInsert() {
    return rawInsert;
  }

  /**
   * @return A SELECT CQL statement with placeholders tenant, timeSlot, seriesSetHash, starting
   * timestamp, ending timestamp and returns timestamp, value
   */
  public String rawQuery() {
    return rawQuery;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot
   */
  public String getRawDelete() {
    return rawDelete;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot, seriesSetHash
   */
  public String getRawDeleteWithSeriesSetHash() {
    return rawDeleteWithSeriesSetHash;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot
   */
  public String getRawGetHashSeriesSetHashQuery() {
    return rawGetSeriesSetHashQuery;
  }

  /**
   * @return an INSERT CQL statement with placeholders tenant, timeSlot, seriesSetHash, aggregator,
   * timestamp, value
   */
  public String downsampleInsert(Duration granularity) {
    return downsampleInserts.get(granularity);
  }

  /**
   * @return an DELETE CQL statement with placeholders tenant, timeSlot
   */
  public String downsampleDelete(Duration granularity) {
    return downsampleDeletes.get(granularity);
  }

  /**
   * @return an DELETE CQL statement with placeholders tenant, timeSlot, seriesSetHash
   */
  public String downsampleDeleteWithSeriesSetHash(Duration granularity) {
    return downsampleDeletesWithSeriesSetHash.get(granularity);
  }

  /**
   * @return a SELECT CQL statement with placeholders tenant, timeSlot, seriesSetHash, aggregator,
   * starting timestamp, ending timestamp and returns timestamp, value
   */
  public String downsampleQuery(Duration granularity) {
    return downsampleQueries.get(granularity);
  }
}
