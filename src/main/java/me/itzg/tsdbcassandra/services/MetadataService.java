package me.itzg.tsdbcassandra.services;

import static org.springframework.data.cassandra.core.query.Criteria.where;
import static org.springframework.data.cassandra.core.query.Query.query;

import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class MetadataService {

  private final ReactiveCqlTemplate cqlTemplate;
  private final ReactiveCassandraTemplate cassandraTemplate;

  @Autowired
  public MetadataService(ReactiveCqlTemplate cqlTemplate,
                         ReactiveCassandraTemplate cassandraTemplate) {
    this.cqlTemplate = cqlTemplate;
    this.cassandraTemplate = cassandraTemplate;
  }

  public Publisher<?> storeMetadata(String tenant, Metric metric, String seriesSet) {
    return
        cassandraTemplate.update(
            query(
                where("tenant").is(tenant),
                where("metricName").is(metric.getMetric())
            ),
            Update.empty().addTo("aggregators").append(Aggregator.raw),
            MetricName.class
        ).and(
            Flux.fromIterable(metric.getTags().entrySet())
                .flatMap(tagsEntry ->
                    Flux.concat(
                        cassandraTemplate.insert(
                            new TagKey()
                                .setTenant(tenant)
                                .setMetricName(metric.getMetric())
                                .setTagKey(tagsEntry.getKey())
                        ),
                        cassandraTemplate.insert(
                            new TagValue()
                                .setTenant(tenant)
                                .setMetricName(metric.getMetric())
                                .setTagKey(tagsEntry.getKey())
                                .setTagValue(tagsEntry.getValue())
                        ),
                        cassandraTemplate.insert(
                            new SeriesSet()
                                .setTenant(tenant)
                                .setMetricName(metric.getMetric())
                                .setTagKey(tagsEntry.getKey())
                                .setTagValue(tagsEntry.getValue())
                                .setSeriesSet(seriesSet)
                        )
                    )
                )
        );
  }

  public Mono<List<String>> getMetricNames(String tenant) {
    return cqlTemplate.queryForFlux(
        "SELECT metric_name FROM metric_names WHERE tenant = ?",
        String.class,
        tenant
    ).collectList();
  }

  /**
   * Determines if the requested metricName has the requested aggregator tracked with it.
   * @param tenant tenant scope of the metric name
   * @param metricName the metric name
   * @param aggregator an aggregator to check
   * @return true if the metric name exists and has requested aggregator
   */
  public Mono<Boolean> metricNameHasAggregator(String tenant, String metricName, Aggregator aggregator) {
    return cassandraTemplate.exists(
        query(
            where("tenant").is(tenant),
            where("metricName").is(metricName),
            where("aggregators").contains(aggregator)
        )
            // for aggregators part
            .withAllowFiltering(),
        MetricName.class
    );
  }

  public Mono<List<String>> getTagKeys(String tenant, String metricName) {
    return cqlTemplate.queryForFlux(
        "SELECT tag_key FROM tag_keys WHERE tenant = ? AND metric_name = ?",
        String.class,
        tenant, metricName
    ).collectList();
  }

  public Mono<List<String>> getTagValues(String tenant, String metricName, String tagKey) {
    return cqlTemplate.queryForFlux(
        "SELECT tag_value FROM tag_values"
            + " WHERE tenant = ? AND metric_name = ? AND tag_key = ?",
        String.class,
        tenant, metricName, tagKey
    ).collectList();
  }

  public Mono<Set<String>> locateSeriesSets(String tenant, String metricName,
                                            Map<String, String> queryTags) {
    return Flux.fromIterable(queryTags.entrySet())
        // find the series-sets for each query tag
        .flatMap(tagEntry ->
            cqlTemplate.queryForFlux(
                "SELECT series_set FROM series_sets"
                    + " WHERE tenant = ? AND metric_name = ? AND tag_key = ? AND tag_value = ?",
                String.class,
                tenant, metricName, tagEntry.getKey(), tagEntry.getValue()
            )
                .collect(Collectors.toSet())
        )
        // and reduce to the intersection of those
        .reduce((results1, results2) ->
            results1.stream()
                .filter(results2::contains)
                .collect(Collectors.toSet())
        );
  }

  public Mono<?> updateMetricNames(String tenant, String metricName, Set<Aggregator> aggregators) {
    return cassandraTemplate.update(
        query(
            where("tenant").is(tenant),
            where("metricName").is(metricName)
        ),
        Update.empty().addTo("aggregators").appendAll(aggregators),
        MetricName.class
    );
  }
}
