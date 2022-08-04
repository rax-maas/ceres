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

import com.rackspace.ceres.app.config.configValidator.GranularityValidator;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationFormat;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;

@ConfigurationProperties("ceres.downsample")
@Component
@Data
@Validated
public class DownsampleProperties {
  /**
   * Specifies how many partitions should be used when tracking ingested data for downsampling.
   * A value of zero disables downsample tracking.
   */
  @Min(0)
  int partitions;

  public boolean isTrackingEnabled() {
    return partitions > 0;
  }

  /**
   * The amount of time to wait after startup before the downsample processing starts.
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration initialProcessingDelay = Duration.ofSeconds(5);

  /**
   * The number of threads allocated to process downsampling partitions.
   * The default uses number of available processors reported by the JVM.
   */
  @Min(1)
  int processingThreads = Runtime.getRuntime().availableProcessors();

  /**
   * Max time a downsampling job can be locked
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration maxDownsampleJobDuration = Duration.ofMinutes(1);

  Duration maxDelayedInProgress = Duration.ofHours(6);

  /**
   * Max time a downsampling delayed job can be locked
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration maxDownsampleDelayedJobDuration = Duration.ofMinutes(1);

  /**
   * Specifies how wide in time the jobs will be spread out
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration downsampleSpreadPeriod = Duration.ofMinutes(1);

  /**
   * Specifies how often delayed timeslots will be processed
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration downsampleDelayedSpreadPeriod = Duration.ofMinutes(1);

  /**
   * Target granularities to downsample from raw data.
   */
  @GranularityValidator
  List<Granularity> granularities;

  /**
   * Maximum size of the cache that tracks hashes per timeslot.
   */
  @Min(0)
  long downsampleHashCacheSize = 2000000;

  @Data
  public static class Granularity {

    /**
     * The width of the downsample target where the timestamps will be normalized to multiples of
     * this width.
     * For example: 5m
     */
    @NotNull
    @DurationFormat(DurationStyle.SIMPLE)
    Duration width;

    /**
     * The amount of time to retain this granularity of downsamples.
     * For example: 14d
     */
    @NotNull
    @DurationFormat(DurationStyle.SIMPLE)
    Duration ttl;

    /**
     * The approximate number of compaction windows to declare on a newly created data table.
     * Cassandra documentation recommends 20-30 windows:
     * https://cassandra.apache.org/doc/latest/operating/compaction/twcs.html#twcs
     */
    @Min(20)
    int compactionWindows = 30;

    /**
     * The width of time slots used for partitioning data, which can reduce the number
     * of Cassandra files that need to be scanned.
     */
    @NotNull
    Duration partitionWidth;
  }
}
