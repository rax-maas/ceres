package me.itzg.tsdbcassandra.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("app")
@Component
@Data
@Validated
public class AppProperties {
  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration rawTtl = Duration.ofHours(6);
}
