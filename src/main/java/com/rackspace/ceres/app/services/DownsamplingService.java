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
