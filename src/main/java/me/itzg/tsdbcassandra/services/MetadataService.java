package me.itzg.tsdbcassandra.services;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  public Mono<List<String>> getMetricNames(String tenant) {
    return cqlTemplate.queryForFlux(
        "SELECT metric_name FROM metric_names WHERE tenant = ?",
        String.class,
        tenant
    ).collectList();
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

  public Mono<List<String>> locateSeriesSets(String tenant, String metricName,
                                             Map<String, String> queryTags) {
    return Flux.fromIterable(queryTags.entrySet())
        .flatMap(tagEntry ->
            cqlTemplate.queryForFlux(
                "SELECT series_set FROM series_sets"
                    + " WHERE tenant = ? AND metric_name = ? AND tag_key = ? AND tag_value = ?",
                String.class,
                tenant, metricName, tagEntry.getKey(), tagEntry.getValue()
            )
                .collectList()
        )
        .reduce((results1, results2) ->
            results1.stream()
                .filter(results2::contains)
                .collect(Collectors.toList())
        );
  }
}
