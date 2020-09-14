package me.itzg.tsdbcassandra.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
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

  @javax.validation.constraints.Pattern(regexp = "(\\d+|(\\d+-\\d+))(,(\\d+|(\\d+-\\d+)))*",
      message = "Needs to be comma separated list of integers or integer ranges")
  String partitionsToProcess;

  @NotNull @NotEmpty
  List<Granularity> granularities = List.of(
      new Granularity().setWidth(Duration.ofMinutes(5)).setTtl(Duration.ofHours(48))
  );

  @Data
  public static class Granularity {
    Duration width;
    Duration ttl;
  }

  public List<Integer> expandPartitionsToProcess() {
    if (!StringUtils.hasText(partitionsToProcess)) {
      return List.of();
    }

    final Pattern pattern = Pattern.compile("(?<range>(?<start>\\d+)-(?<end>\\d+))|(?<single>\\d+)");
    final String[] parts = partitionsToProcess.split(",");

    final ArrayList<Integer> partitions = new ArrayList<>();
    for (String part : parts) {
      final Matcher m = pattern.matcher(part);
      if (m.matches()) {
        if (m.group("single") != null) {
          partitions.add(Integer.parseInt(m.group("single")));
        } else {
          for (int i = Integer.parseInt(m.group("start")); i <= Integer.parseInt(m.group("end")); i++) {
            partitions.add(i);
          }
        }
      } else {
        throw new IllegalArgumentException("Invalid expression in partitionsToProcess");
      }
    }

    return partitions;
  }
}
