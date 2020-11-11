package com.rackspace.ceres.app.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
public class DateTimeUtilsTest {

  @Test
  public void getAbsoluteTimeFromRelativeTimeTest() {
    Instant actual = DateTimeUtils.getAbsoluteTimeFromRelativeTime("1s-ago");
    Instant expected = Instant.now().minus(1, ChronoUnit.SECONDS);
    assertThat(Duration.between(actual, expected).getSeconds()).isLessThanOrEqualTo(1);
  }

  @Test
  public void getAbsoluteTimeFromRelativeTimeTest_Invalid() {
    assertThatThrownBy(() -> DateTimeUtils.getAbsoluteTimeFromRelativeTime("1ss-ago"))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid relative time format");
  }

  @Test
  public void isValidInstantInstanceTest() {
    assertThat(DateTimeUtils.isValidInstantInstance("2020-11-10T14:24:35Z")).isTrue();
  }

  @Test
  public void isValidInstantInstanceTest_Invalid() {
    assertThat(DateTimeUtils.isValidInstantInstance("13:03:15.454+0530Z")).isFalse();
  }

  @Test
  public void isValidEpochMillisTest() {
    assertThat(DateTimeUtils.isValidEpochMillis("1605094715000")).isTrue();
  }

  @Test
  public void isValidEpochMillisTest_Invalid() {
    assertThat(DateTimeUtils.isValidEpochMillis("1605094715")).isFalse();
  }

  @Test
  public void isValidEpochSecondsTest() {
    assertThat(DateTimeUtils.isValidEpochSeconds("1605094715")).isTrue();
  }

  @Test
  public void isValidEpochSecondsTest_Invalid() {
    assertThat(DateTimeUtils.isValidEpochSeconds("1605094715000")).isFalse();
  }

  @Test
  public void parseInstantTestWithNull() {
    assertThat(DateTimeUtils.parseInstant(null)).isNotNull();
  }

  @Test
  public void parseInstantTestWithEpochMillis() {
    assertThat(DateTimeUtils.parseInstant("1605094715000"))
        .isEqualTo(Instant.ofEpochMilli(1605094715000l));
  }

  @Test
  public void parseInstantTestWithEpochSeconds() {
    assertThat(DateTimeUtils.parseInstant("1605094715"))
        .isEqualTo(Instant.ofEpochSecond(1605094715));
  }

  @Test
  public void parseInstantTestWithUTCTime() {
    assertThat(DateTimeUtils.parseInstant("2020-11-10T14:24:35Z"))
        .isEqualTo(Instant.parse("2020-11-10T14:24:35Z"));
  }

  @Test
  public void parseInstantTestWithRelativeTime() {
    assertThat(DateTimeUtils.parseInstant("1s-ago")).isNotNull();
  }
}
