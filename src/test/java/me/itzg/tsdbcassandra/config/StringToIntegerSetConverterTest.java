package me.itzg.tsdbcassandra.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringToIntegerSetConverterTest {

  @Test
  void combo() {
    final StringToIntegerSetConverter converter = new StringToIntegerSetConverter();

    final IntegerSet results = converter.convert("1,5-8,12,15-18");

    assertThat(results).containsExactly(
        1, 5, 6, 7, 8, 12, 15, 16, 17, 18
    );
  }

  @Test
  void nullInput() {
    final StringToIntegerSetConverter converter = new StringToIntegerSetConverter();

    final IntegerSet results = converter.convert(null);

    assertThat(results).isNotNull();
    assertThat(results).hasSize(0);
  }

  @Test
  void blankInput() {
    final StringToIntegerSetConverter converter = new StringToIntegerSetConverter();

    final IntegerSet results = converter.convert("");

    assertThat(results).isNotNull();
    assertThat(results).hasSize(0);
  }
}
