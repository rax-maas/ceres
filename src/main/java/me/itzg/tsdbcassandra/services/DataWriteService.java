package me.itzg.tsdbcassandra.services;

import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import me.itzg.tsdbcassandra.config.AppProperties;
import me.itzg.tsdbcassandra.entities.DataDownsampled;
import me.itzg.tsdbcassandra.entities.DataRaw;
import me.itzg.tsdbcassandra.model.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.UpdateOptions;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Service
@Slf4j
public class DataWriteService {

  private final ReactiveCassandraTemplate cassandraTemplate;
  private final SeriesSetService seriesSetService;
  private final MetadataService metadataService;
  private final DownsampleTrackingService downsampleTrackingService;
  private final AppProperties appProperties;

  @Autowired
  public DataWriteService(ReactiveCassandraTemplate cassandraTemplate,
                          SeriesSetService seriesSetService,
                          MetadataService metadataService,
                          DownsampleTrackingService downsampleTrackingService,
                          AppProperties appProperties) {
    this.cassandraTemplate = cassandraTemplate;
    this.seriesSetService = seriesSetService;
    this.metadataService = metadataService;
    this.downsampleTrackingService = downsampleTrackingService;
    this.appProperties = appProperties;
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
    return cassandraTemplate.insert(
        new DataRaw()
            .setTenant(tenant)
            .setSeriesSet(seriesSet)
            .setTs(metric.getTimestamp())
            .setValue(metric.getValue().doubleValue()),
        InsertOptions.builder()
            .ttl(appProperties.getRawTtl())
            .build()
    );
  }

  public Flux<Tuple2<DataDownsampled,Boolean>> storeDownsampledData(Flux<DataDownsampled> data, Duration ttl) {
    return data.flatMap(entry -> cassandraTemplate.update(
        entry,
        UpdateOptions.builder()
            .ttl(ttl)
            .build()
        )
    )
        .doOnNext(result -> log
            .trace("Stored downsampled={} applied={}", result.getEntity(), result.wasApplied()))
        .map(result -> Tuples.of(result.getEntity(), result.wasApplied()));
  }
}
