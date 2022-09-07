package com.rackspace.ceres.app.services;

import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.downsample.AggregatedValueSet;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@Slf4j
@Profile({"query", "downsample"})
public class DownsampleWriteService
{
  private final ReactiveCqlTemplate cqlTemplate;
  private final DataTablesStatements dataTablesStatements;
  private final TimeSlotPartitioner timeSlotPartitioner;
  private final AppProperties appProperties;
  private final Counter dbOperationErrorsCounter;

  @Autowired
  public DownsampleWriteService(ReactiveCqlTemplate cqlTemplate,
                                DataTablesStatements dataTablesStatements,
                                TimeSlotPartitioner timeSlotPartitioner,
                                AppProperties appProperties,
                                MeterRegistry meterRegistry) {
    this.cqlTemplate = cqlTemplate;
    this.dataTablesStatements = dataTablesStatements;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.appProperties = appProperties;
    this.dbOperationErrorsCounter = meterRegistry.counter("ceres.db.operation.errors", "type", "write");
  }

  /**
   * Stores a batch of downsampled data where it is assumed the flux contains data
   * of the same tenant, series-set, and granularity.
   *
   * @param data flux of data to be stored in a downsampled data table
   * @return a mono that completes when the batch is stored
   */
  public Mono<?> storeDownsampledData(Flux<AggregatedValueSet> data, String tenant, String seriesSet) {
    return data
        // convert each data point to an insert-statement
        .map(entry ->
            new SimpleStatementBuilder(dataTablesStatements.downsampleInsert(entry.getGranularity()))
                .addPositionalValues(
                    // TENANT, TIME_PARTITION_SLOT, SERIES_SET_HASH, TIMESTAMP, MIN, MAX, SUM, AVG, COUNT
                    tenant,
                    timeSlotPartitioner.downsampledTimeSlot(entry.getTimestamp(), entry.getGranularity()),
                    seriesSet,
                    entry.getTimestamp(),
                    entry.getMin(),
                    entry.getMax(),
                    entry.getSum(),
                    entry.getAverage(),
                    entry.getCount()
                )
                .build()
        )
        .collectList()
        // ...and create a batch statement containing those
        .map(statements -> {
          final BatchStatementBuilder batchStatementBuilder = new BatchStatementBuilder(
              BatchType.LOGGED);
          // NOTE: tried addStatements, but unable to cast iterables
          statements.forEach(batchStatementBuilder::addStatement);
          return batchStatementBuilder.build();
        })
        // ...and execute the batch
        .flatMap(cqlTemplate::execute)
        .retryWhen(appProperties.getRetryInsertDownsampled().build())
        .doOnError(e -> dbOperationErrorsCounter.increment())
        .checkpoint();
  }
}
