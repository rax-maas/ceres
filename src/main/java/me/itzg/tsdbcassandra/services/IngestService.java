package me.itzg.tsdbcassandra.services;

import java.time.Duration;
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
public class IngestService {

  private final ReactiveCassandraTemplate cassandraTemplate;
  private final SeriesSetService seriesSetService;
  private final MetadataService metadataService;
  private final DownsampleTrackingService downsampleTrackingService;
  private final AppProperties appProperties;

  @Autowired
  public IngestService(ReactiveCassandraTemplate cassandraTemplate,
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

  public Mono<Metric> ingest(Metric metric) {

    final String seriesSet = seriesSetService
        .buildSeriesSet(metric.getMetricName(), metric.getTags());

    return
        storeRawData(metric, seriesSet)
            .and(metadataService.storeMetadata(metric, seriesSet))
            .and(downsampleTrackingService.track(metric, seriesSet))
            .then(Mono.just(metric));
  }

  private Mono<?> storeRawData(Metric metric, String seriesSet) {
    return cassandraTemplate.insert(
        new DataRaw()
            .setTenant(metric.getTenant())
            .setSeriesSet(seriesSet)
            .setTs(metric.getTs())
            .setValue(metric.getValue()),
        InsertOptions.builder()
            .ttl(appProperties.getRawTtl())
            .build()
    );
  }

  public Flux<Tuple2<DataDownsampled,Boolean>> storeDownsampledData(Flux<DataDownsampled> data, Duration ttl) {
    return data.flatMap(entry -> cassandraTemplate.update(entry, UpdateOptions.builder()
        .ttl(ttl)
        .build()))
        .map(result -> Tuples.of(result.getEntity(), result.wasApplied()));
  }
}
