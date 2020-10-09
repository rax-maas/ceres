package me.itzg.ceres.downsample;

import java.time.Duration;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import lombok.Getter;

public class TemporalNormalizer implements TemporalAdjuster {

  @Getter
  final Duration rounding;

  public TemporalNormalizer(Duration rounding) {
    this.rounding = rounding;
  }

  @Override
  public Temporal adjustInto(Temporal temporal) {
    return temporal
        .with(ChronoField.NANO_OF_SECOND, 0)
        .with(ChronoField.INSTANT_SECONDS, round(temporal.getLong(ChronoField.INSTANT_SECONDS)));
  }

  private long round(long seconds) {
    return seconds - (seconds % rounding.getSeconds());
  }
}
