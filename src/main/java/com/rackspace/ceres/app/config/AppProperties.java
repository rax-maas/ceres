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

package com.rackspace.ceres.app.config;

import com.rackspace.ceres.app.model.TagFilter;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("ceres")
@Component
@Data
@Validated
public class AppProperties {
  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration rawTtl = Duration.ofHours(6);

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration downsamplingHashesTtl = Duration.ofHours(12);

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration delayedHashesTtl = Duration.ofHours(12);

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration downsamplingHashesCacheTtl = Duration.ofHours(1);

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration downsamplingTimeslotCacheTtl = Duration.ofMinutes(5);

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration delayedHashesCacheTtl = Duration.ofMinutes(5);

  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration delayedTimeslotCacheTtl = Duration.ofMinutes(5);

  /**
   * The delay factor applied to delay regular downsampling periods to minimize delayed timeslots
   */
  float downsampleDelayFactor = 1.4F;

  /**
   * When initially creating the Cassandra data table schemas,
   * this value will be used for the expired data garbage collection.
   */
  @Min(60)
  int dataTableGcGraceSeconds = 86400;

  @Min(60)
  int delayedHashesGcGraceSeconds = 60;

  /**
   * The approximate number of compaction windows to declare on a newly created raw data table.
   * Cassandra documentation recommends 20-30 windows:
   * https://cassandra.apache.org/doc/latest/operating/compaction/twcs.html#twcs
   */
  @Min(20)
  int rawCompactionWindows = 30;

  /**
   * The width of time slots used for partitioning data, which can reduce the number
   * of Cassandra files that need to be scanned. It is best to set this to the same
   * as <code>ceres.downsample.timeSlotWidth</code> or greater to ensure downsampling
   * queries only need to consult one partition.
   */
  @NotNull
  Duration rawPartitionWidth = Duration.ofHours(1);

  /**
   * Identifies the tenant for ingest and query API calls. For ingest this header is optional
   * and instead <code>tenant-tag</code> will be used or the configured <code>default-tenant</code>
   * as a fallback.
   */
  String tenantHeader = "X-Tenant";
  /**
   * When the tenant header is not present during ingest, a tag with this key will be used. If the tag
   * is present, it is removed from the tags of the metric since tenant is stored as a distinct
   * column.
   */
  String tenantTag = "tenant";

  /**
   * Maximum size of the cache that tracks series-sets that have been persisted into Cassandra.
   */
  @Min(0)
  long seriesSetCacheSize = 2000000;

  @NotNull
  RetrySpec retryInsertMetadata = new RetrySpec()
      .setMaxAttempts(5)
      .setMinBackoff(Duration.ofMillis(100));

  @NotNull
  RetrySpec retryInsertRaw = new RetrySpec()
      .setMaxAttempts(5)
      .setMinBackoff(Duration.ofMillis(100));

  @NotNull
  RetrySpec retryInsertDownsampled = new RetrySpec()
      .setMaxAttempts(5)
      .setMinBackoff(Duration.ofMillis(100));

  @NotNull
  RetrySpec retryQueryForDownsample = new RetrySpec()
      .setMaxAttempts(5)
      .setMinBackoff(Duration.ofMillis(100));

  @NotNull
  RetrySpec retryDelete = new RetrySpec()
      .setMaxAttempts(3)
      .setMinBackoff(Duration.ofMillis(100));

  @NotNull TagFilter tagFilter = TagFilter.EXCLUDE;

  @NotNull Integer tagValueLimit = 50;

  List<String> excludedTagKeys;

  Duration ingestStartTime = Duration.ofDays(7);

  Duration ingestEndTime = Duration.ofDays(1);
}
