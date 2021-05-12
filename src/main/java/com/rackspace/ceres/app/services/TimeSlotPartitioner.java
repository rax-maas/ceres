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
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import com.rackspace.ceres.app.model.TsdbQuery;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
public class TimeSlotPartitioner {

  private final Map<Duration, Entry> downsampleEntries;
  private final Entry rawEntry;

  private static class Entry {

    final TemporalNormalizer normalizer;
    final Duration partitionWidth;

    private Entry(TemporalNormalizer normalizer, Duration partitionWidth) {
      this.normalizer = normalizer;
      this.partitionWidth = partitionWidth;
    }
  }

  @Autowired
  public TimeSlotPartitioner(AppProperties appProperties,
                             DownsampleProperties downsampleProperties) {
    rawEntry = new Entry(
        new TemporalNormalizer(appProperties.getRawPartitionWidth()),
        appProperties.getRawPartitionWidth()
    );

    if (downsampleProperties.getGranularities() != null) {
      downsampleEntries = downsampleProperties.getGranularities().stream()
          .collect(Collectors.toMap(
              Granularity::getWidth,
              granularity -> new Entry(
                  new TemporalNormalizer(granularity.getPartitionWidth()),
                  granularity.getPartitionWidth()
              )
          ));
    }
    else {
      downsampleEntries = Map.of();
    }
  }

  public Instant rawTimeSlot(Instant ts) {
    return ts.with(rawEntry.normalizer);
  }

  public Instant downsampledTimeSlot(Instant ts, Duration granularity) {
    final Entry entry = downsampleEntries.get(granularity);
    if (entry != null) {
      return ts.with(entry.normalizer);
    }
    throw new IllegalArgumentException("Unknown downsample granularity: " + granularity);
  }

  /**
   * @param start start of range, inclusive
   * @param end end of range, exclusive
   * @param granularity the downsample granularity or null for raw
   * @return the partition time slots
   */
  public Iterable<Instant> partitionsOverRange(Instant start, Instant end, Duration granularity) {
    final Entry entry;
    if (granularity == null) {
      entry = rawEntry;
    } else {
      entry = downsampleEntries.get(granularity);
      if (entry == null) {
        throw new IllegalArgumentException("Unknown downsample granularity: " + granularity);
      }
    }

    final List<Instant> partitions = new ArrayList<>();

    Instant current = start.with(entry.normalizer);
    partitions.add(current);

    for (Instant next = current.plus(entry.partitionWidth);
        // use isBefore since 'end' is exclusive
        next.isBefore(end);
        next = next.plus(entry.partitionWidth)
    ) {
      partitions.add(next);
    }

    return partitions;
  }

  public Flux<Instant> partitionsOverRangeFromQuery(Instant start, Instant end, Duration granularity) {
    return Flux.fromIterable(partitionsOverRange(start, end, granularity));
  }
}
