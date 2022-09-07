package com.rackspace.ceres.app.services;

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.downsample.AggregatedValueSet;
import com.rackspace.ceres.app.downsample.SingleValueSet;
import com.rackspace.ceres.app.downsample.ValueSet;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;

import static com.rackspace.ceres.app.utils.DateTimeUtils.getLowerGranularity;
import static com.rackspace.ceres.app.utils.DateTimeUtils.isLowerGranularityRaw;

@Service
@Slf4j
@Profile({"query", "downsample"})
public class DownsampleQueryService {
  private final ReactiveCqlTemplate cqlTemplate;
  private final DataTablesStatements dataTablesStatements;
  private final TimeSlotPartitioner timeSlotPartitioner;
  private final DownsampleProperties properties;
  private final AppProperties appProperties;

  public DownsampleQueryService(ReactiveCqlTemplate cqlTemplate,
                                DataTablesStatements dataTablesStatements,
                                TimeSlotPartitioner timeSlotPartitioner,
                                DownsampleProperties properties,
                                AppProperties appProperties) {
    this.cqlTemplate = cqlTemplate;
    this.dataTablesStatements = dataTablesStatements;
    this.timeSlotPartitioner = timeSlotPartitioner;
    this.properties = properties;
    this.appProperties = appProperties;
  }

  /**
   * Fetches data points to be downsampled. The data points are based on the next lower granularity downsampled data
   * and if there is no lower downsampled data it fetches the raw data points. This is to avoid always fetching
   * raw which might be a performance and memory problem if we are downsampling very large granularities like e.g. 24h.
   */
  public Flux<ValueSet> fetchData(PendingDownsampleSet set, String group, Duration granularity, boolean redoOld) {
    Duration lowerWidth = getLowerGranularity(this.properties.getGranularities(), granularity);
    return isLowerGranularityRaw(lowerWidth) ?
        queryRawWithSeriesSet(
            set.getTenant(),
            set.getSeriesSetHash(),
            set.getTimeSlot(),
            set.getTimeSlot().plus(Duration.parse(group))
        )
        :
        queryDownsampled(
            set.getTenant(),
            set.getSeriesSetHash(),
            redoOld ? set.getTimeSlot().minus(Duration.parse(group)) : set.getTimeSlot(),
            set.getTimeSlot().plus(Duration.parse(group)),
            lowerWidth);
  }

  private Flux<ValueSet> queryDownsampled(
      String tenant, String seriesSet, Instant start, Instant end, Duration granularity) {
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, granularity))
        .concatMap(timeSlot ->
            cqlTemplate.queryForRows(dataTablesStatements.downsampleQuery(granularity),
                    tenant,
                    timeSlot,
                    seriesSet,
                    start,
                    end)
                .name("queryDownsampledWithSeriesSet")
                .metrics()
                .retryWhen(this.appProperties.getRetryQueryForDownsample().build())
                .map(row -> // TIMESTAMP, MIN, MAX, SUM, AVG, COUNT
                    new AggregatedValueSet()
                        .setMin(row.getDouble(1))
                        .setMax(row.getDouble(2))
                        .setSum(row.getDouble(3))
                        .setAverage(row.getDouble(4))
                        .setCount(row.getInt(5))
                        .setGranularity(granularity)
                        .setTimestamp(row.getInstant(0))
                )
        )
        .checkpoint();
  }

  public Flux<ValueSet> queryRawWithSeriesSet(String tenant, String seriesSet,
                                              Instant start, Instant end) {
    return Flux.fromIterable(timeSlotPartitioner.partitionsOverRange(start, end, null))
        .concatMap(timeSlot ->
            cqlTemplate.queryForRows(dataTablesStatements.rawQuery(), tenant, timeSlot, seriesSet, start, end)
                .name("queryRawWithSeriesSet")
                .metrics()
                .retryWhen(this.appProperties.getRetryQueryForDownsample().build())
                .map(row -> new SingleValueSet().setValue(row.getDouble(1)).setTimestamp(row.getInstant(0)))
        )
        .checkpoint();
  }
}
