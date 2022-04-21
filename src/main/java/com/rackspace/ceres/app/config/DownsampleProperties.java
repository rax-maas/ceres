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
import java.nio.file.Path;
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

  @DurationUnit(ChronoUnit.HOURS)
  Duration timeSlotMaxWidth = Duration.ofHours(1);

  @DurationUnit(ChronoUnit.MINUTES)
  Duration timeSlotMinWidth = Duration.ofMinutes(30);

  /**
   * The amount of time to allow for the lastTouch of pending downsample sets to remain stable.
   * This is mainly useful for handling backfilled data where this delay ensures that all metrics
   * have likely been ingested for the slot.
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration lastTouchDelay = Duration.ofMinutes(5);

  /**
   * The amount of time to wait after startup before the downsample processing starts.
   * Each partition is staggered by up to an additional 50% of this duration.
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
   * Specifies how often pending downsample sets will be retrieved and processed.
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration downsampleProcessPeriod = Duration.ofMinutes(1);

  /**
   * Comma separated list of partitions or ranges of partitions, such as "0,5-8,12,15-18"
   */
  IntegerSet partitionsToProcess;

  /**
   * Can be used instead of <code>partitionsToProcess</code> to provide a shared JSON file
   * that contains a map of hostname to comma separated list of partitions or ranges of partitions.
   * This property is ignored if <code>partitionsToProcess</code> is configured.
   */
  Path partitionsMappingFile;

  /**
   * Target granularities to downsample from raw data.
   */
  @GranularityValidator
  List<Granularity> granularities;

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
