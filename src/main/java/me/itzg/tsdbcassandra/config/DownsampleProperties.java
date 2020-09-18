package me.itzg.tsdbcassandra.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationFormat;
import org.springframework.boot.convert.DurationStyle;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("app.downsample")
@Component
@Data
@Validated
public class DownsampleProperties {
  /**
   * Specifies how many partitions should be used when tracking ingested data for downsampling.
   * A value of zero disables downsample tracking.
   */
  @Min(0)
  int partitions;

  public boolean isTrackingEnabled() {
    return partitions > 0;
  }

  @DurationUnit(ChronoUnit.HOURS)
  Duration timeSlotWidth = Duration.ofHours(1);

  /**
   * The amount of time to allow for the lastTouch of pending downsample sets to remain stable.
   * This is mainly useful for handling backfilled data where this delay ensures that all metrics
   * have likely been ingested for the slot.
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration lastTouchDelay = Duration.ofMinutes(5);

  /**
   * Specifies how often pending downsample sets will be retrieved and processed.
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration downsampleProcessPeriod = Duration.ofMinutes(1);

  int pendingRetrievalLimit = 10;

  @DurationUnit(ChronoUnit.SECONDS)
  Duration pendingRetrieveRepeatDelay = Duration.ofSeconds(1);

  /**
   * Comma separated list of partitions or ranges of partitions, such as "0,5-8,12,15-18"
   */
  IntegerSet partitionsToProcess;

  /**
   * Target granularities to downsample from raw data.
   */
  List<Granularity> granularities;

  @Data
  public static class Granularity {

    /**
     * The width of the downsample target where the timestamps will be normalized to multiples of
     * this width.
     * For example: 5m
     */
    @NotNull
    @DurationFormat(DurationStyle.SIMPLE)
    Duration width;

    /**
     * The amount of time to retain this granularity of downsamples.
     * For example: 14d
     */
    @NotNull
    @DurationFormat(DurationStyle.SIMPLE)
    Duration ttl;
  }
}
