package me.itzg.ceres.downsample;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class ValueSetCollectorsTest {

  @Test
  void gaugeCollector() {
    final AggregatedValueSet result = Stream.of(
        new SingleValueSet().setValue(1.2).setTimestamp(Instant.parse("2007-12-03T10:15:31.01Z")),
        new SingleValueSet().setValue(2.5).setTimestamp(Instant.parse("2007-12-03T10:16:23.02Z")),
        new SingleValueSet().setValue(3.1).setTimestamp(Instant.parse("2007-12-03T10:17:12.03Z")),
        new SingleValueSet().setValue(1.1).setTimestamp(Instant.parse("2007-12-03T10:18:56.04Z"))
    )
        .collect(ValueSetCollectors.gaugeCollector(Duration.ofMinutes(5)));

    assertThat(result.getMin()).isEqualTo(1.1);
    assertThat(result.getMax()).isEqualTo(3.1);
    assertThat(result.getSum()).isEqualTo(7.9);
    assertThat(result.getCount()).isEqualTo(4);
    assertThat(result.getAverage()).isEqualTo(1.975);
    assertThat(result.getTimestamp()).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    assertThat(result.getGranularity()).isEqualTo(Duration.ofMinutes(5));
  }

  @Test
  void counterCollector() {
    final AggregatedValueSet result = Stream.of(
        new SingleValueSet().setValue(12).setTimestamp(Instant.parse("2007-12-03T10:35:31.01Z")),
        new SingleValueSet().setValue(25).setTimestamp(Instant.parse("2007-12-03T10:36:23.02Z")),
        new SingleValueSet().setValue(31).setTimestamp(Instant.parse("2007-12-03T10:37:12.03Z")),
        new SingleValueSet().setValue(11).setTimestamp(Instant.parse("2007-12-03T10:38:56.04Z"))
    )
        .collect(ValueSetCollectors.counterCollector(Duration.ofMinutes(5)));

    assertThat(result.getSum()).isEqualTo(79);
    assertThat(result.getTimestamp()).isEqualTo(Instant.parse("2007-12-03T10:35:00.00Z"));
    assertThat(result.getGranularity()).isEqualTo(Duration.ofMinutes(5));
  }
}
