package me.itzg.tsdbcassandra.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.validation.constraints.Min;
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
}
