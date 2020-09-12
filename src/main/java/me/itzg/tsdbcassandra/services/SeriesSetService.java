package me.itzg.tsdbcassandra.services;

import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import me.itzg.tsdbcassandra.entities.Aggregator;
import me.itzg.tsdbcassandra.entities.MetricName;
import me.itzg.tsdbcassandra.entities.SeriesSet;
import me.itzg.tsdbcassandra.entities.TagKey;
import me.itzg.tsdbcassandra.entities.TagValue;
import me.itzg.tsdbcassandra.model.Metric;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
public class SeriesSetService {

  private final ReactiveCassandraTemplate cassandraTemplate;

  @Autowired
  public SeriesSetService(ReactiveCassandraTemplate cassandraTemplate) {
    this.cassandraTemplate = cassandraTemplate;
  }

  public Publisher<?> storeMetadata(Metric metric, String seriesSet) {
    return
        cassandraTemplate.update(
            query(
                where("tenant").is(metric.getTenant()),
                where("metricName").is(metric.getMetricName())
            ),
            Update.empty().addTo("aggregators").append(Aggregator.raw),
            MetricName.class
        ).and(
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
        );
  }

  public String buildSeriesSet(String metricName, Map<String, String> tags) {
    return metricName + "," +
        tags.entrySet().stream()
            .sorted(Entry.comparingByKey())
            .map(tagsEntry -> tagsEntry.getKey() + "=" + tagsEntry.getValue())
            .collect(Collectors.joining(","));
  }
}
