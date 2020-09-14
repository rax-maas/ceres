package me.itzg.tsdbcassandra.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("app.downsample")
@Component
@Data
@Validated
public class DownsampleProperties {
  boolean enabled;

  @Min(1)
  int partitions = 1;

  @DurationUnit(ChronoUnit.HOURS)
  Duration timeSlotWidth = Duration.ofHours(1);

  /**
   * The amount of time to allow for the lastTouch of pending downsample sets to remain stable.
   * This is mainly useful for handling backfilled data where this delay ensures that all metrics
   * have likely been ingested for the slot.
   */
  @DurationUnit(ChronoUnit.MINUTES)
  Duration lastTouchDelay = Duration.ofMinutes(5);

  @DurationUnit(ChronoUnit.MINUTES)
  Duration downsampleProcessPeriod = Duration.ofMinutes(1);

  /**
   * Comma separated list of partitions or ranges of partitions, such as "0,5-8,12,15-18"
   */
  IntegerSet partitionsToProcess;

  @NotNull @NotEmpty
  List<Granularity> granularities = List.of(
      new Granularity().setWidth(Duration.ofMinutes(5)).setTtl(Duration.ofHours(48))
  );

  @Data
  public static class Granularity {
    Duration width;
    Duration ttl;
  }
}
