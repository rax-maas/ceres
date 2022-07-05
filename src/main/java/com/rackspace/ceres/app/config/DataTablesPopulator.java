/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.ceres.app.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.rackspace.ceres.app.services.DataTablesStatements;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DefaultOption;
import org.springframework.data.cassandra.core.cql.keyspace.Option;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption;
import org.springframework.data.cassandra.core.cql.keyspace.TableOption.CompactionOption;
import org.springframework.data.cassandra.core.cql.session.init.KeyspacePopulator;
import org.springframework.data.cassandra.core.cql.session.init.ScriptException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Handles the configuration-driven creation of data table schemas with corresponding table
 * options for default TTL, time-window compaction strategy, and time-window sizes.
 * @see DataTablesStatements
 */
@Component
@Slf4j
public class DataTablesPopulator implements KeyspacePopulator {

  private static final DefaultOption COMPACTION_WINDOW_UNIT = new DefaultOption("compaction_window_unit", String.class, true, false, true);
  /**
   * value is number of the compaction_window_unit increments.
   */
  private static final DefaultOption COMPACTION_WINDOW_SIZE = new DefaultOption("compaction_window_size", Long.class, true, false, false);
  private static final String DEFAULT_TIME_TO_LIVE = "default_time_to_live";

  private final AppProperties appProperties;
  private final DownsampleProperties downsampleProperties;
  private final DataTablesStatements dataTablesStatements;

  @Autowired
  public DataTablesPopulator(AppProperties appProperties,
                             DownsampleProperties downsampleProperties,
                             DataTablesStatements dataTablesStatements) {
    this.appProperties = appProperties;
    this.downsampleProperties = downsampleProperties;
    this.dataTablesStatements = dataTablesStatements;
  }

  @Override
  public void populate(CqlSession session) throws ScriptException {
    dataDownsampledTableSpecs()
        .concatWithValues(dataRawTableSpec(appProperties.getRawTtl()))
        .flatMap(spec -> createTable(spec, session))
            .flatMap( spec -> createTable(downsamplingHashesTableSpec(appProperties.getRawTtl()), session))
        .subscribe();
  }

  public Mono<List<String>> render() {
    return dataDownsampledTableSpecs()
        .concatWithValues(dataRawTableSpec(appProperties.getRawTtl()))
        .map(CreateTableCqlGenerator::toCql)
        .collectList();
  }

  private Flux<CreateTableSpecification> dataDownsampledTableSpecs() {
    return downsampleProperties.getGranularities() == null ? Flux.empty() :
        Flux.fromStream(
            downsampleProperties.getGranularities().stream()
                .map(granularity -> dataDownsampledTableSpec(
                    granularity.getWidth(),
                    granularity.getTtl(),
                    granularity.getPartitionWidth()
                ))
        );
  }

  private Publisher<?> createTable(CreateTableSpecification createTableSpec,
                                   CqlSession session) {
    return session.executeReactive(CreateTableCqlGenerator.toCql(createTableSpec));
  }

  private CreateTableSpecification dataDownsampledTableSpec(Duration width, Duration ttl,
                                                            Duration partitionWidth) {
    return CreateTableSpecification
        .createTable(dataTablesStatements.tableNameDownsampled(width, partitionWidth))
        .ifNotExists()
        .partitionKeyColumn(DataTablesStatements.TENANT, DataTypes.TEXT)
        .partitionKeyColumn(DataTablesStatements.TIME_PARTITION_SLOT, DataTypes.TIMESTAMP)
        .clusteredKeyColumn(DataTablesStatements.SERIES_SET_HASH, DataTypes.TEXT)
        .clusteredKeyColumn(DataTablesStatements.TIMESTAMP, DataTypes.TIMESTAMP)
        .column(DataTablesStatements.MIN, DataTypes.DOUBLE)
        .column(DataTablesStatements.MAX, DataTypes.DOUBLE)
        .column(DataTablesStatements.SUM, DataTypes.DOUBLE)
        .column(DataTablesStatements.AVG, DataTypes.DOUBLE)
        .column(DataTablesStatements.COUNT, DataTypes.INT)
        .with(DEFAULT_TIME_TO_LIVE, ttl.getSeconds(), false, false)
        .with(TableOption.COMPACTION, compactionOptions(ttl))
        .with(TableOption.GC_GRACE_SECONDS, appProperties.getDataTableGcGraceSeconds());
  }

  private CreateTableSpecification dataRawTableSpec(Duration ttl) {
    return CreateTableSpecification
        .createTable(dataTablesStatements.tableNameRaw(appProperties.getRawPartitionWidth()))
        .ifNotExists()
        .partitionKeyColumn(DataTablesStatements.TENANT, DataTypes.TEXT)
        .partitionKeyColumn(DataTablesStatements.TIME_PARTITION_SLOT, DataTypes.TIMESTAMP)
        .clusteredKeyColumn(DataTablesStatements.SERIES_SET_HASH, DataTypes.TEXT)
        .clusteredKeyColumn(DataTablesStatements.TIMESTAMP, DataTypes.TIMESTAMP)
        .column(DataTablesStatements.VALUE, DataTypes.DOUBLE)
        .with(DEFAULT_TIME_TO_LIVE, ttl.getSeconds(), false, false)
        .with(TableOption.COMPACTION, compactionOptions(ttl))
        .with(TableOption.GC_GRACE_SECONDS, appProperties.getDataTableGcGraceSeconds());
  }

  private CreateTableSpecification downsamplingHashesTableSpec(Duration ttl) {
    log.info("creating downsampling_hashes table");
    return CreateTableSpecification
            .createTable("downsampling_hashes")
            .ifNotExists()
            .partitionKeyColumn("partition", DataTypes.INT)
            .clusteredKeyColumn("hash", DataTypes.TEXT)
            .with(DEFAULT_TIME_TO_LIVE, ttl.getSeconds(), false, false);
  }

  private Map<Option,Object> compactionOptions(Duration ttl) {

    // Docs recommend 20 - 30 windows
    final Duration calculatedWindowSize = ttl.dividedBy(30);

    // ...pick a TimeUnit that seems appropriate for scale
    final TimeUnit windowUnit;
    final long windowSize;
    if (calculatedWindowSize.compareTo(Duration.ofDays(1)) > 0) {
      windowUnit = TimeUnit.DAYS;
      windowSize = calculatedWindowSize.toDays();
    } else if (calculatedWindowSize.compareTo(Duration.ofHours(1)) > 0) {
      windowUnit = TimeUnit.HOURS;
      windowSize = calculatedWindowSize.toHours();
    } else {
      windowUnit = TimeUnit.MINUTES;
      windowSize = calculatedWindowSize.toMinutes();
    }

    return Map.of(
        CompactionOption.CLASS, "TimeWindowCompactionStrategy",
        COMPACTION_WINDOW_UNIT, windowUnit,
        COMPACTION_WINDOW_SIZE, nicerWindowSizeValues(windowSize)
    );
  }

  /**
   * Avoids ugly values like 7 hours or 11 days and rounds to 10's, 5, or original value otherwise
   * @param value a possibly ugly value
   * @return a nicer value
   */
  private static long nicerWindowSizeValues(long value) {
    if (value >= 10) {
      return value - (value % 10);
    } else if (value >= 5) {
      return 5;
    } else {
      return value;
    }
  }
}
