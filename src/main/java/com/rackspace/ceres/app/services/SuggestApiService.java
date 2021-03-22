package com.rackspace.ceres.app.services;

import java.util.List;
import javax.xml.crypto.Data;
import jnr.ffi.annotations.Meta;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class SuggestApiService {

  private final DataTablesStatements dataTablesStatements;
  private final ReactiveCqlTemplate cqlTemplate;
  private final MetadataService metadataService;

  @Autowired
  public SuggestApiService(DataTablesStatements dataTablesStatements, ReactiveCqlTemplate cqlTemplate,
      MetadataService metadataService) {
    this.dataTablesStatements = dataTablesStatements;
    this.cqlTemplate = cqlTemplate;
    this.metadataService = metadataService;
  }

  public Mono<List<String>> suggestTagKeys(String tenant, String tagK, int max) {
    return metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable)
        .flatMap(metric -> metadataService.getTagKeys(tenant, metric).flatMapMany(Flux::fromIterable)
        .filter(tagKey -> !StringUtils.hasText(tagK) || tagKey.startsWith(tagK)))
        .limitRequest(max)
        .collectList();
    //return cqlTemplate.queryForFlux(cqlStatement, String.class, tenantId).filter(str -> str.startsWith(tagK)).distinct();
  }

  public Mono<List<String>> suggestMetricNames(String tenant, String metric, int max) {
    return metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable)
        .filter(metricName -> !StringUtils.hasText(metric) || metricName.startsWith(metric))
        .limitRequest(max)
        .collectList();
  }

  public Mono<List<String>> suggestTagValues(String tenant, String tagV, int max) {
    return metadataService.getMetricNames(tenant).flatMapMany(Flux::fromIterable)
        .flatMap(metric -> metadataService.getTagKeys(tenant, metric).flatMapMany(Flux::fromIterable)
        .flatMap(tagK -> metadataService.getTagValues(tenant, metric, tagK).flatMapMany(Flux::fromIterable)))
        .filter(tagValue -> !StringUtils.hasText(tagV) || tagValue.startsWith(tagV))
        .limitRequest(max)
        .collectList();
  }
}
