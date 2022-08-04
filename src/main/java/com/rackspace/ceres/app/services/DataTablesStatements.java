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
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Provides a consolidated declaration of insert and query statements that execute against the
 * configuration-driven data tables schema.
 */
@Component
public class DataTablesStatements {

  public static final String TENANT = "tenant";
  public static final String TIME_PARTITION_SLOT = "time_slot";
  public static final String SERIES_SET_HASH = "series_set_hash";
  public static final String TIMESTAMP = "ts";
  public static final String MIN = "min";
  public static final String MAX = "max";
  public static final String SUM = "sum";
  public static final String AVG = "avg";
  public static final String COUNT = "count";
  public static final String VALUE = "value";

  private static final String TABLE_PREFIX = "data";
  private static final String RAW = "raw";

  private final Map<Duration, String> downsampleInserts = new HashMap<>();
  private final Map<Duration, String> downsampleQueries = new HashMap<>();
  private final Map<Duration, String> downsampleDeletes = new HashMap<>();
  private final Map<Duration, String> downsampleDeletesWithSeriesSetHash = new HashMap<>();

  //CQL Queries
  private String RAW_INSERT = "INSERT INTO %s ( %s ) VALUES (?, ?, ?, ?, ?)";

  private String RAW_QUERY = "SELECT %s FROM %s WHERE " + TENANT + " = ?"
      + "  AND " + TIME_PARTITION_SLOT + " = ? AND " + SERIES_SET_HASH + " = ?"
      + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?";

  private String RAW_DELETE = "DELETE FROM %s WHERE " + TENANT + " = ?"
      + "  AND " + TIME_PARTITION_SLOT + " = ?";

  private String RAW_GET_SERIES_SET_HASH_QUERY = "SELECT series_set_hash FROM %s "
      + " WHERE " + TENANT + " = ? AND " + TIME_PARTITION_SLOT + " = ?";

  private String RAW_DELETE_WITH_SERIES_SET_HASH = "DELETE FROM %s WHERE " + TENANT + " = ?"
      + "  AND " + TIME_PARTITION_SLOT + " = ? AND series_set_hash = ?";

  private String DOWNSAMPLE_INSERT = "INSERT INTO %s ( %s ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private String DOWNSAMPLE_QUERY = "SELECT %s FROM %s WHERE " + TENANT + " = ?"
      + "  AND " + TIME_PARTITION_SLOT + " = ? AND " + SERIES_SET_HASH + " = ?"
      + "  AND " + TIMESTAMP + " >= ? AND " + TIMESTAMP + " < ?";

  private String DOWNSAMPLE_DELETE = "DELETE FROM %s WHERE " + TENANT + " = ?"
      + " AND " + TIME_PARTITION_SLOT + " = ?";

  private String DOWNSAMPLE_DELETE_WITH_SERIES_SET_HASH = "DELETE FROM %s WHERE " + TENANT + " = ?"
      + "  AND " + TIME_PARTITION_SLOT + " = ? AND series_set_hash = ?";

  private String DOWNSAMPLED_GET_SERIES_SET_HASH_QUERY = "SELECT series_set_hash FROM %s WHERE "
      + TENANT + " = ? AND " + TIME_PARTITION_SLOT + " = ?";

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
    RAW_INSERT = String.format(RAW_INSERT,
        tableNameRaw(appProperties.getRawPartitionWidth()),
        String.join(",", TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, TIMESTAMP, VALUE));

    RAW_QUERY = String.format(RAW_QUERY,
        String.join(",", TIMESTAMP, VALUE),
        tableNameRaw(appProperties.getRawPartitionWidth()));

    RAW_DELETE = String.format(RAW_DELETE,
        tableNameRaw(appProperties.getRawPartitionWidth()));

    RAW_DELETE_WITH_SERIES_SET_HASH = String.format(RAW_DELETE_WITH_SERIES_SET_HASH,
        tableNameRaw(appProperties.getRawPartitionWidth()));

    RAW_GET_SERIES_SET_HASH_QUERY = String.format(RAW_GET_SERIES_SET_HASH_QUERY,
        tableNameRaw(appProperties.getRawPartitionWidth()));
  }

  private void buildDownsampleStatements(DownsampleProperties downsampleProperties) {
    if (downsampleProperties.getGranularities() == null) {
      return;
    }

    downsampleProperties.getGranularities()
        .forEach(granularity -> {

          downsampleInserts.put(granularity.getWidth(),
              String.format(DOWNSAMPLE_INSERT, tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth()), String.join(",",
                  TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, TIMESTAMP, MIN, MAX, SUM, AVG, COUNT)));

          downsampleQueries.put(granularity.getWidth(),
              String.format(DOWNSAMPLE_QUERY, String.join(",", TIMESTAMP, MIN, MAX, SUM, AVG, COUNT),
                  tableNameDownsampled(granularity.getWidth(), granularity.getPartitionWidth())));

          downsampleDeletes.put(granularity.getWidth(),
              String.format(DOWNSAMPLE_DELETE, tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth())));

          downsampleDeletesWithSeriesSetHash.put(granularity.getWidth(),
              String.format(DOWNSAMPLE_DELETE_WITH_SERIES_SET_HASH, tableNameDownsampled(granularity.getWidth(),
                  granularity.getPartitionWidth())));
        });

    Granularity granularity = downsampleProperties.getGranularities()
        .stream().max(Comparator.comparing(Granularity::getTtl)).get();

    DOWNSAMPLED_GET_SERIES_SET_HASH_QUERY = String.format(DOWNSAMPLED_GET_SERIES_SET_HASH_QUERY,
        tableNameDownsampled(granularity.getWidth(),
            granularity.getPartitionWidth()));
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
    return RAW_INSERT;
  }

  /**
   * @return A SELECT CQL statement with placeholders tenant, timeSlot, seriesSetHash, starting
   * timestamp, ending timestamp and returns timestamp, value
   */
  public String rawQuery() {
    return RAW_QUERY;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot
   */
  public String getRawDelete() {
    return RAW_DELETE;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot, seriesSetHash
   */
  public String getRawDeleteWithSeriesSetHash() {
    return RAW_DELETE_WITH_SERIES_SET_HASH;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot from raw table
   */
  public String getRawGetHashSeriesSetHashQuery() {
    return RAW_GET_SERIES_SET_HASH_QUERY;
  }

  /**
   * @return A DELETE CQL statement with placeholders tenant, timeSlot from downsampled table
   */
  public String getDownsampledGetHashQuery() {
    return DOWNSAMPLED_GET_SERIES_SET_HASH_QUERY;
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
