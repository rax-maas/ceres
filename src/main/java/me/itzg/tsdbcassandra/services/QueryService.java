package me.itzg.tsdbcassandra.services;

import static java.util.Objects.requireNonNull;

import com.datastax.oss.driver.api.core.cql.Row;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import me.itzg.tsdbcassandra.downsample.SingleValueSet;
import me.itzg.tsdbcassandra.downsample.ValueSet;
import me.itzg.tsdbcassandra.entities.Aggregator;
import me.itzg.tsdbcassandra.model.MetricNameAndTags;
import me.itzg.tsdbcassandra.model.QueryResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class QueryService {

  private final ReactiveCqlTemplate cqlTemplate;
  private final MetadataService metadataService;
  private final SeriesSetService seriesSetService;

  @Autowired
  public QueryService(ReactiveCqlTemplate cqlTemplate,
                      MetadataService metadataService,
                      SeriesSetService seriesSetService) {
    this.cqlTemplate = cqlTemplate;
    this.metadataService = metadataService;
    this.seriesSetService = seriesSetService;
  }

  public Flux<QueryResult> queryRaw(String tenant, String metricName,
                                    Map<String, String> queryTags,
                                    Instant start, Instant end) {
    return metadataService.locateSeriesSets(tenant, metricName, queryTags)
        .name("queryRaw")
        .metrics()
        .flatMapMany(Flux::fromIterable)
        .flatMap(seriesSet ->
            mapSeriesSetResult(tenant, seriesSet,
                // TODO use repository and projections
                cqlTemplate.queryForRows(
                    "SELECT ts, value FROM data_raw"
                        + " WHERE tenant = ? AND series_set = ?"
                        + "  AND ts >= ? AND ts < ?",
                    tenant, seriesSet, start, end
                )
            )
        );
  }

  public Flux<ValueSet> queryRawWithSeriesSet(String tenant, String seriesSet,
                                              Instant start, Instant end) {
    // TODO use repository and projections
    return cqlTemplate.queryForRows(
        "SELECT ts, value FROM data_raw"
            + " WHERE tenant = ? AND series_set = ?"
            + "  AND ts >= ? AND ts < ?",
        tenant, seriesSet, start, end
    )
        .map(row ->
            new SingleValueSet().setValue(row.getDouble(1)).setTimestamp(row.getInstant(0))
        );
  }

  public Flux<QueryResult> queryDownsampled(String tenant, String metricName, Aggregator aggregator,
                                            Duration granularity, Map<String, String> queryTags,
                                            Instant start, Instant end) {
    return metadataService.locateSeriesSets(tenant, metricName, queryTags)
        .name("queryDownsampled")
        .metrics()
        .flatMapMany(Flux::fromIterable)
        .flatMap(seriesSet ->
            mapSeriesSetResult(tenant, seriesSet,
                // TODO use repository and projections
                cqlTemplate.queryForRows(
                    "SELECT ts, value FROM data_downsampled"
                        + " WHERE"
                        + "  tenant = ?"
                        + "  AND series_set = ?"
                        + "  AND aggregator = ?"
                        + "  AND granularity = ?"
                        + "  AND ts >= ? AND ts < ?",
                    tenant, seriesSet, aggregator.name(), granularity.toString(), start, end
                )
            )
        )
        .checkpoint();
  }

  private Mono<QueryResult> mapSeriesSetResult(String tenant, String seriesSet, Flux<Row> rows) {
    return rows
        .map(row -> Map.entry(
            requireNonNull(row.getInstant(0)),
            row.getDouble(1)
            )
        )
        // collect the ts->value entries into an ordered, LinkedHashMap
        .collectMap(Entry::getKey, Entry::getValue, LinkedHashMap::new)
        .filter(values -> !values.isEmpty())
        .map(values -> buildQueryResult(tenant, seriesSet, values));
  }

  private QueryResult buildQueryResult(String tenant, String seriesSet,
                                       Map<Instant, Double> values) {
    final MetricNameAndTags metricNameAndTags = seriesSetService.expandSeriesSet(seriesSet);

    return new QueryResult()
        .setTenant(tenant)
        .setMetricName(metricNameAndTags.getMetricName())
        .setTags(metricNameAndTags.getTags())
        .setValues(values);
  }
}
