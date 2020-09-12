package me.itzg.tsdbcassandra.services;

import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

import java.util.Map.Entry;
import java.util.stream.Collectors;
import me.itzg.tsdbcassandra.config.AppProperties;
import me.itzg.tsdbcassandra.entities.Aggregator;
import me.itzg.tsdbcassandra.entities.DataRaw;
import me.itzg.tsdbcassandra.entities.MetricName;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.entities.TagKey;
import me.itzg.tsdbcassandra.entities.TagValue;
import me.itzg.tsdbcassandra.model.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class IngestService {

  private final ReactiveCassandraTemplate cassandraTemplate;
  private final SeriesSetService seriesSetService;
  private final AppProperties appProperties;

  @Autowired
  public IngestService(ReactiveCassandraTemplate cassandraTemplate,
                       SeriesSetService seriesSetService,
                       AppProperties appProperties) {
    this.cassandraTemplate = cassandraTemplate;
    this.seriesSetService = seriesSetService;
    this.appProperties = appProperties;
  }

  public Mono<?> ingest(Metric metric) {

    final String seriesSet = seriesSetService
        .buildSeriesSet(metric.getMetricName(), metric.getTags());

    return
        insertData(metric, seriesSet)
            .and(seriesSetService.storeMetadata(metric, seriesSet));
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
