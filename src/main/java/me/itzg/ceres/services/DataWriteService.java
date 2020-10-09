package me.itzg.ceres.services;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import me.itzg.ceres.downsample.DataDownsampled;
import me.itzg.ceres.model.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Service
@Slf4j
public class DataWriteService {

  private final ReactiveCqlTemplate cqlTemplate;
  private final SeriesSetService seriesSetService;
  private final MetadataService metadataService;
  private final DataTablesStatements dataTablesStatements;
  private final DownsampleTrackingService downsampleTrackingService;

  @Autowired
  public DataWriteService(ReactiveCqlTemplate cqlTemplate,
                          SeriesSetService seriesSetService,
                          MetadataService metadataService,
                          DataTablesStatements dataTablesStatements,
                          DownsampleTrackingService downsampleTrackingService) {
    this.cqlTemplate = cqlTemplate;
    this.seriesSetService = seriesSetService;
    this.metadataService = metadataService;
    this.dataTablesStatements = dataTablesStatements;
    this.downsampleTrackingService = downsampleTrackingService;
  }

  public Flux<Metric> ingest(Flux<Tuple2<String,Metric>> metrics) {
    return metrics.flatMap(tuple -> ingest(tuple.getT1(), tuple.getT2()));
  }

  public Mono<Metric> ingest(String tenant, Metric metric) {
    final String seriesSet = seriesSetService
        .buildSeriesSet(metric.getMetric(), metric.getTags());

    return
        storeRawData(tenant, metric, seriesSet)
            .name("ingest")
            .metrics()
            .and(metadataService.storeMetadata(tenant, metric, seriesSet))
            .and(downsampleTrackingService.track(tenant, seriesSet, metric.getTimestamp()))
            .then(Mono.just(metric));
  }

  private Mono<?> storeRawData(String tenant, Metric metric, String seriesSet) {
    return cqlTemplate.execute(
        dataTablesStatements.insertRaw(),
        tenant, seriesSet, metric.getTimestamp(), metric.getValue().doubleValue()
    );
  }

  public Flux<Tuple2<DataDownsampled,Boolean>> storeDownsampledData(Flux<DataDownsampled> data, Duration ttl) {
    return data.flatMap(entry -> cqlTemplate.execute(
        dataTablesStatements.insertDownsampled(entry.getGranularity()),
        entry.getTenant(), entry.getSeriesSet(), entry.getAggregator().name(),
        entry.getTs(), entry.getValue()
        )
            .map(applied -> Tuples.of(entry, applied))
    );
  }
}
