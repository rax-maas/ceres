package me.itzg.tsdbcassandra.services;

import me.itzg.tsdbcassandra.config.AppProperties;
import me.itzg.tsdbcassandra.entities.DataRaw;
import me.itzg.tsdbcassandra.model.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class IngestService {

  private final ReactiveCassandraTemplate cassandraTemplate;
  private final SeriesSetService seriesSetService;
  private final DownsampleTrackingService downsampleTrackingService;
  private final AppProperties appProperties;

  @Autowired
  public IngestService(ReactiveCassandraTemplate cassandraTemplate,
                       SeriesSetService seriesSetService,
                       DownsampleTrackingService downsampleTrackingService,
                       AppProperties appProperties) {
    this.cassandraTemplate = cassandraTemplate;
    this.seriesSetService = seriesSetService;
    this.downsampleTrackingService = downsampleTrackingService;
    this.appProperties = appProperties;
  }

  public Mono<Metric> ingest(Metric metric) {

    final String seriesSet = seriesSetService
        .buildSeriesSet(metric.getMetricName(), metric.getTags());

    return
        insertData(metric, seriesSet)
            .and(seriesSetService.storeMetadata(metric, seriesSet))
            .and(downsampleTrackingService.track(metric, seriesSet))
            .then(Mono.just(metric));
  }

  private Mono<?> insertData(Metric metric, String seriesSet) {
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
}
