/*
 * Copyright 2022 Rackspace US, Inc.
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

import com.rackspace.ceres.app.downsample.AggregatedValueSet;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.downsample.ValueSetCollectors;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Service
@Slf4j
public class DownsamplingService {
  private final DataWriteService dataWriteService;

  @Autowired
  public DownsamplingService(DataWriteService dataWriteService) {
    this.dataWriteService = dataWriteService;
  }

  public Mono<?> downsampleData(PendingDownsampleSet pendingSet, Duration granularity, Flux<ValueSet> data) {
    final Flux<AggregatedValueSet> aggregated = data
        .windowUntilChanged(valueSet ->
            valueSet.getTimestamp().with(new TemporalNormalizer(granularity)), Instant::equals)
        .concatMap(valueSetFlux -> valueSetFlux.collect(ValueSetCollectors.gaugeCollector(granularity)));

    return dataWriteService.storeDownsampledData(aggregated, pendingSet.getTenant(), pendingSet.getSeriesSetHash())
        .checkpoint();
  }
}
