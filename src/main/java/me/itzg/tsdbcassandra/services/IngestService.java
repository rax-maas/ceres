package me.itzg.tsdbcassandra.services;

import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import me.itzg.tsdbcassandra.config.AppProperties;
import me.itzg.tsdbcassandra.entities.DataRaw;
import me.itzg.tsdbcassandra.entities.MetricName;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.entities.TagKey;
import me.itzg.tsdbcassandra.entities.TagValue;
import me.itzg.tsdbcassandra.model.Metric;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.EntityWriteResult;
import org.springframework.data.cassandra.core.InsertOptions;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class IngestService {

  private final ReactiveCassandraTemplate cassandraTemplate;
  private final AppProperties appProperties;

  @Autowired
  public IngestService(ReactiveCassandraTemplate cassandraTemplate,
                       AppProperties appProperties) {
    this.cassandraTemplate = cassandraTemplate;
    this.appProperties = appProperties;
  }

  public Mono<?> ingest(Metric metric) {

    final String seriesSet = buildSeriesSet(metric);

    return
        insertData(metric, seriesSet)
            .then(
                insertMetadata(metric, seriesSet)
            );
  }

  private Mono<?> insertData(Metric metric, String seriesSet) {
    return cassandraTemplate.insert(
        new DataRaw()
            .setTenant(metric.getTenant())
            .setSeriesSet(seriesSet)
            .setTs(metric.getTs())
            .setValue(metric.getValue()),
        InsertOptions.builder()
            // calculate TTL relative to metric's timestamp
            .ttl(
                Duration.between(Instant.now(), metric.getTs()).plus(appProperties.getRawTtl()))
            .build()
    );
  }

  private Mono<?> insertMetadata(Metric metric, String seriesSet) {
    return cassandraTemplate.insert(
        new MetricName()
            .setTenant(metric.getTenant())
            .setMetricName(metric.getMetricName())
    )
        .thenMany(
            Flux.fromIterable(metric.getTags().entrySet())
                .flatMap(tagsEntry ->
                    Flux.concat(
                        cassandraTemplate.insert(
                            new TagKey()
                                .setTenant(metric.getTenant())
                                .setMetricName(metric.getMetricName())
                                .setTagKey(tagsEntry.getKey())
                        ),
                        cassandraTemplate.insert(
                            new TagValue()
                                .setTenant(metric.getTenant())
                                .setMetricName(metric.getMetricName())
                                .setTagKey(tagsEntry.getKey())
                                .setTagValue(tagsEntry.getValue())
                        ),
                        cassandraTemplate.insert(
                            new SeriesSet()
                                .setTenant(metric.getTenant())
                                .setMetricName(metric.getMetricName())
                                .setTagKey(tagsEntry.getKey())
                                .setTagValue(tagsEntry.getValue())
                                .setSeriesSet(seriesSet)
                        )
                    )
                )
        )
        .ignoreElements();
  }

  private String buildSeriesSet(Metric metric) {
    return metric.getMetricName() + "," +
        metric.getTags().entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(tagsEntry -> tagsEntry.getKey() + "=" + tagsEntry.getValue())
            .collect(Collectors.joining(","));
  }
}
