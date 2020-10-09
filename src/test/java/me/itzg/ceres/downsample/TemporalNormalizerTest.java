package me.itzg.ceres.downsample;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class TemporalNormalizerTest {

  @Nested
  class roundTo5m {
    @Test
    void withinTheMinute() {
      final Instant result = Instant.parse("2007-12-03T10:15:32.08Z")
          .with(new TemporalNormalizer(Duration.ofMinutes(5)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    }

    @Test
    void outsideTheMinute() {
      final Instant result = Instant.parse("2007-12-03T10:17:23.45Z")
          .with(new TemporalNormalizer(Duration.ofMinutes(5)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    }

    @Test
    void alreadyAligned() {
      final Instant result = Instant.parse("2007-12-03T10:15:00.00Z")
          .with(new TemporalNormalizer(Duration.ofMinutes(5)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:15:00.00Z"));
    }
  }

  @Nested
  class roundTo1h {

    @Test
    void normal() {
      final Instant result = Instant.parse("2007-12-03T10:15:32.08Z")
          .with(new TemporalNormalizer(Duration.ofHours(1)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:00:00.00Z"));
    }

    @Test
    void alreadyAligned() {
      final Instant result = Instant.parse("2007-12-03T10:00:00.00Z")
          .with(new TemporalNormalizer(Duration.ofHours(1)));

      assertThat(result).isEqualTo(Instant.parse("2007-12-03T10:00:00.00Z"));
    }
  }
}
