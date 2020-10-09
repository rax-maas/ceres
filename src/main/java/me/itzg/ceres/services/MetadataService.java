package me.itzg.ceres.services;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import me.itzg.ceres.entities.MetricName;
import me.itzg.ceres.entities.SeriesSet;
import me.itzg.ceres.model.Metric;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
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
        cassandraTemplate.insert(
            new MetricName().setTenant(tenant).setMetricName(metric.getMetric())
        )
            .and(
                Flux.fromIterable(metric.getTags().entrySet())
                    .flatMap(tagsEntry ->
                        Flux.concat(
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

  public Mono<List<String>> getTenants() {
    return cqlTemplate.queryForFlux(
        "SELECT tenant FROM metric_names"
            // use GROUP BY since unable to SELECT DISTINCT on primary key column
            + " GROUP BY tenant",
        String.class
    ).collectList();
  }

  public Mono<List<String>> getMetricNames(String tenant) {
    return cqlTemplate.queryForFlux(
        "SELECT metric_name FROM metric_names WHERE tenant = ?",
        String.class,
        tenant
    ).collectList();
  }

  public Mono<List<String>> getTagKeys(String tenant, String metricName) {
    return cqlTemplate.queryForFlux(
        "SELECT tag_key FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ?"
            // use GROUP BY since unable to SELECT DISTINCT on primary key column
            + " GROUP BY tag_key",
        String.class,
        tenant, metricName
    ).collectList();
  }

  public Mono<List<String>> getTagValues(String tenant, String metricName, String tagKey) {
    return cqlTemplate.queryForFlux(
        "SELECT tag_value FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ? AND tag_key = ?"
            // use GROUP BY since unable to SELECT DISTINCT on primary key column
            + " GROUP BY tag_value",
        String.class,
        tenant, metricName, tagKey
    ).collectList();
  }

  /**
   * Locates the recorded series-sets (<code>metricName,tagK=tagV,...</code>) that match the given
   * search criteria.
   * @param tenant series-sets are located by this tenant
   * @param metricName series-sets are located by this metric name
   * @param queryTags series-sets are located by and'ing these tag key-value pairs
   * @return the matching series-sets
   */
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

}
