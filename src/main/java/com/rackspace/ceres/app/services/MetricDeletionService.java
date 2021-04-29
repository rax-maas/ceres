package com.rackspace.ceres.app.services;


import static com.rackspace.ceres.app.web.TagListConverter.convertPairsListToMap;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@Profile("downsample")
public class MetricDeletionService {

  private static final String PREFIX_SERIES_SET_HASHES = "seriesSetHashes";
  private static final String DELIM = "|";

  private final ReactiveCqlTemplate cqlTemplate;
  private final DataTablesStatements dataTablesStatements;
  private AppProperties appProperties;
  private TimeSlotPartitioner timeSlotPartitioner;
  private DownsampleProperties downsampleProperties;
  private final ReactiveStringRedisTemplate redisTemplate;

  @Autowired
  public MetricDeletionService(ReactiveCqlTemplate cqlTemplate,
      DataTablesStatements dataTablesStatements, AppProperties appProperties,
      TimeSlotPartitioner timeSlotPartitioner,
      DownsampleProperties downsampleProperties, ReactiveStringRedisTemplate redisTemplate) {
    this.cqlTemplate = cqlTemplate;
    this.dataTablesStatements = dataTablesStatements;
    this.appProperties = appProperties;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.downsampleProperties = downsampleProperties;
    this.redisTemplate = redisTemplate;
  }

  public Mono<Void> deleteMetrics(String tenant, String metricName, List<String> tag,
      Instant start, Instant end) {
    if (StringUtils.isAllBlank(metricName)) {
      return deleteMetricsByTenantId(tenant, start, end);
    } else if (CollectionUtils.isEmpty(tag)) {
      return deleteMetricsByMetricName(tenant, metricName, start, end);
    } else {
      return deleteMetricsByMetricNameAndTag(tenant, metricName, tag, start, end);
    }
  }

  private Mono<Void> deleteMetricsByTenantId(String tenant, Instant start, Instant end) {
    log.info("inside deleteMetricsByTenantId method");
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          return deleteMetricsByTenantId(downsampleProperties.getGranularities(), tenant, timeSlot);
        }).then();
  }

  private Mono<Void> deleteMetricsByMetricName(String tenant, String metricName, Instant start,
      Instant end) {
    log.info("inside deleteMetricsByMetricName method");
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashedEntries = getSeriesSetHashFromSeriesSets(tenant,
              metricName);
          return deleteMetric(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashedEntries);
        }).then();
  }

  private Mono<Void> deleteMetricsByMetricNameAndTag(String tenant, String metricName,
      List<String> tag, Instant start, Instant end) {
    log.info("inside deleteMetricsByMetricNameAndTag method");
    Map<String, String> queryTags = convertPairsListToMap(tag);
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .flatMap(timeSlot -> {
          Flux<String> seriesSetHashedEntries = getSeriesSetHashFromSeriesSets(tenant,
              metricName, queryTags);
          return deleteMetric(downsampleProperties.getGranularities(), tenant, timeSlot, metricName,
              seriesSetHashedEntries);
        }).then();
  }

  private Mono<Boolean> deleteMetric(List<Granularity> granularities, String tenant,
      Instant timeSlot, String metricName, Flux<String> seriesSetHashedEntries) {
    return seriesSetHashedEntries.flatMap(seriesSetHashEntry -> {
      return Flux.fromIterable(granularities).flatMap(granularity -> {
        return cqlTemplate
            .execute(dataTablesStatements.downsampleDeleteWithSeriesSetHash(granularity.getWidth()),
                tenant,
                timeSlot, seriesSetHashEntry);
      }).then(Mono.just(true));
    }).then(Mono.just(true))
        .flatMap(item -> {
          if (item) {
            return seriesSetHashedEntries.flatMap(seriesSetHashEntry -> {
              return cqlTemplate
                  .execute(dataTablesStatements.getRawDeleteWithSeriesSetHash(), tenant, timeSlot,
                      seriesSetHashEntry);
            }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return deleteMetricNamesByTenantAndMetricName(tenant, metricName);
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return seriesSetHashedEntries.flatMap(seriesSetHashEntry -> {
              return deleteSeriesSetHashes(tenant,
                  seriesSetHashEntry);
            }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return seriesSetHashedEntries.flatMap(seriesSetHashEntry -> {
              return removeEntryFromCache(tenant,
                  seriesSetHashEntry);
            }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return deleteSeriesSetsByTenantIdAndMetricName(tenant, metricName);
          } else {
            return Mono.just(false);
          }
        });
  }

  private Mono<Boolean> deleteMetricsByTenantId(List<Granularity> granularities, String tenant,
      Instant timeSlot) {
    Flux<Map<String, Object>> seriesSetHashedEntries = getSeriesSetHashFromRaw(tenant, timeSlot);
    Flux<Map<String, Object>> metricNameEntries = getMetricNames(tenant);
    return Flux.fromIterable(granularities)
        .flatMap(
            granularity -> {
              return cqlTemplate
                  .execute(dataTablesStatements.downsampleDelete(granularity.getWidth()), tenant,
                      timeSlot);
            }).then(Mono.just(true))
        .flatMap(result -> {
          if (result) {
            return seriesSetHashedEntries.flatMap(seriesSetHashEntry -> {
              return deleteSeriesSetHashes(tenant,
                  seriesSetHashEntry.get("series_set_hash").toString());
            }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return metricNameEntries
                .flatMap(metricNameEntry -> {
                  return deleteSeriesSetsByTenantIdAndMetricName(tenant,
                      metricNameEntry.get("metric_name").toString());
                }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return metricNameEntries
                .flatMap(metricNameEntry -> {
                  return deleteMetricNamesByTenantAndMetricName(tenant,
                      metricNameEntry.get("metric_name").toString());
                }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(result -> {
          if (result) {
            return seriesSetHashedEntries.flatMap(seriesSetHashEntry -> {
              return removeEntryFromCache(tenant,
                  seriesSetHashEntry.get("series_set_hash").toString());
            }).then(Mono.just(true));
          } else {
            return Mono.just(false);
          }
        })
        .flatMap(item -> {
          if (item) {
            return cqlTemplate.execute(dataTablesStatements.getRawDelete(), tenant, timeSlot);
          } else {
            return Mono.just(false);
          }
        });
  }

  private Mono<Boolean> deleteMetricNamesByTenantAndMetricName(String tenant, String metricName) {
    return cqlTemplate
        .execute("DELETE FROM metric_names WHERE tenant = ? AND metric_name = ?",
            tenant, metricName);
  }

  private Mono<Boolean> deleteSeriesSetsByTenantIdAndMetricName(String tenant,
      String metricName) {
    return cqlTemplate
        .execute("DELETE FROM series_sets WHERE tenant = ? AND metric_name = ?"
            , tenant, metricName);
  }

  private Flux<Map<String, Object>> getSeriesSetHashFromRaw(String tenant, Instant timeSlot) {
    return cqlTemplate.queryForFlux(dataTablesStatements.getRawGetHashQuery(), tenant, timeSlot);
  }

  public Flux<String> getSeriesSetHashFromSeriesSets(String tenant,
      String metricName) {
    return cqlTemplate.queryForFlux(
        "SELECT series_set_hash FROM series_sets"
            + " WHERE tenant = ? AND metric_name = ? ",
        String.class,
        tenant, metricName).distinct();
  }

  private static Mono<List<String>> covertString(Collection<Object> values){
    List<String> list = new ArrayList<>();
    values.forEach(e -> list.add(e.toString()));
    return Mono.just(list);
  }

  private Flux<String> getSeriesSetHashFromSeriesSets(String tenant,
      String metricName, Map<String, String> queryTags) {
    return Flux.fromIterable(queryTags.entrySet())
        // find the series-sets for each query tag
        .flatMap(tagEntry ->
            cqlTemplate.queryForFlux(
                "SELECT series_set_hash FROM series_sets"
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
        )
        .flatMapMany(Flux::fromIterable);
  }

  private Flux<Map<String, Object>> getMetricNames(String tenant) {
    return cqlTemplate
        .queryForFlux("SELECT metric_name FROM metric_names WHERE tenant = ?", tenant);
  }

  private Mono<Boolean> deleteSeriesSetHashes(String tenant, String seriesSetHash) {
    return
        cqlTemplate
            .execute("DELETE FROM series_set_hashes WHERE tenant = ? AND series_set_hash = ?",
                tenant, seriesSetHash);
  }

  private Mono<Boolean> removeEntryFromCache(String tenant, String seriesSetHash) {
    return redisTemplate.delete(PREFIX_SERIES_SET_HASHES + DELIM + tenant + DELIM + seriesSetHash)
        .flatMap(result -> {
          if (result > 0) {
            return Mono.just(true);
          } else {
            return Mono.just(false);
          }
        });
  }
}

