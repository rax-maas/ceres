package me.itzg.tsdbcassandra.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DownsamplePropertiesTest {
  @Nested
  class expandPartitionsToProcess {
    @Test
    void combo() {
      final DownsampleProperties downsampleProperties = new DownsampleProperties()
          .setPartitionsToProcess("1,5-8,12,15-18");

      final List<Integer> results = downsampleProperties.expandPartitionsToProcess();

      assertThat(results).containsExactly(
          1,5,6,7,8,12,15,16,17,18
      );
    }

    @Test
    void notSet() {
      final DownsampleProperties downsampleProperties = new DownsampleProperties();

      final List<Integer> results = downsampleProperties.expandPartitionsToProcess();

      assertThat(results).isNotNull();
      assertThat(results).hasSize(0);
    }

    @Test
    void blank() {
      final DownsampleProperties downsampleProperties = new DownsampleProperties()
          .setPartitionsToProcess("");

      final List<Integer> results = downsampleProperties.expandPartitionsToProcess();

      assertThat(results).isNotNull();
      assertThat(results).hasSize(0);
    }
  }
}
