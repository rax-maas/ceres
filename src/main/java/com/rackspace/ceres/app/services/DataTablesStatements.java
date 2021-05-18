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
import com.rackspace.ceres.app.utils.SpringResourceUtils;
import java.io.IOException;
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
      DownsampleProperties downsampleProperties) throws IOException, IllegalArgumentException {
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

  private void buildRawStatements(AppProperties appProperties) throws IOException {
    rawInsert = String
        .format(SpringResourceUtils.readContent("cql-queries/raw_insert_query.cql"),
            tableNameRaw(appProperties.getRawPartitionWidth()), String.join(",", TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, TIMESTAMP, VALUE));
    rawQuery = String
        .format(SpringResourceUtils.readContent("cql-queries/raw_select_query.cql"),
            String.join(",", TIMESTAMP, VALUE), tableNameRaw(appProperties.getRawPartitionWidth()));
    rawDelete = String
        .format(SpringResourceUtils.readContent("cql-queries/raw_delete_query.cql"),
            tableNameRaw(appProperties.getRawPartitionWidth()));
    rawDeleteWithSeriesSetHash = String
        .format(SpringResourceUtils.readContent("cql-queries/raw_delete_with_series_set_hash.cql"),
            tableNameRaw(appProperties.getRawPartitionWidth()));
    rawGetSeriesSetHashQuery = String
        .format(SpringResourceUtils.readContent("cql-queries/raw_select_series_set_hash.cql"),
            tableNameRaw(appProperties.getRawPartitionWidth()));
  }

  private void buildDownsampleStatements(DownsampleProperties downsampleProperties) {
    if (downsampleProperties.getGranularities() == null) {
      return;
    }

    downsampleProperties.getGranularities()
        .forEach(granularity -> {
          try {
            downsampleInserts.put(granularity.getWidth(),
                String
                    .format(SpringResourceUtils.readContent("cql-queries/downsample_insert_query.cql"),
                        tableNameDownsampled(granularity.getWidth(),
                            granularity.getPartitionWidth()),
                        String.join(",", TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, AGGREGATOR,
                            TIMESTAMP, VALUE)));
          } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
          }

          try {
            downsampleQueries.put(granularity.getWidth(),
                  String
                      .format(SpringResourceUtils.readContent("cql-queries/downsample_select_query.cql"),
                          String.join(",", TIMESTAMP, VALUE), tableNameDownsampled(granularity.getWidth(),
                              granularity.getPartitionWidth())));
          } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
          }

          try {
            downsampleDeletes.put(granularity.getWidth(),
                  String
                      .format(SpringResourceUtils.readContent("cql-queries/raw_delete_query.cql"),
                          tableNameDownsampled(granularity.getWidth(),
                              granularity.getPartitionWidth())));
          } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
          }

          try {
            downsampleDeletesWithSeriesSetHash.put(granularity.getWidth(),
                String
                    .format(SpringResourceUtils
                            .readContent("cql-queries/raw_delete_with_series_set_hash.cql"),
                        tableNameDownsampled(granularity.getWidth(),
                            granularity.getPartitionWidth())));
          } catch (IOException e) {
            throw new IllegalArgumentException(e.getMessage());
          }
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
