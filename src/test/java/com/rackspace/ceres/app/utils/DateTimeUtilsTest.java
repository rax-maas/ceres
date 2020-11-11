package com.rackspace.ceres.app.utils;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
public class DateTimeUtilsTest {

  @Test
  public void getAbsoluteTimeFromRelativeTimeTest() {
    Instant actual = DateTimeUtils.getAbsoluteTimeFromRelativeTime("1s-ago");
    assertNotNull(actual);
  }

  @Test
  public void getAbsoluteTimeFromRelativeTimeTest_Invalid() {
    assertThatThrownBy(() -> DateTimeUtils.getAbsoluteTimeFromRelativeTime("1ss-ago"))
        .isInstanceOf(IllegalArgumentException.class).hasMessage("Invalid relative time format");
  }

  @Test
  public void isValidInstantInstanceTest() {
    assertTrue(DateTimeUtils.isValidInstantInstance("2020-11-10T14:24:35Z"));
  }

  @Test
  public void isValidInstantInstanceTest_Invalid() {
    assertFalse(DateTimeUtils.isValidInstantInstance("13:03:15.454+0530Z"));
  }

  @Test
  public void isValidEpochMillisTest() {
    assertTrue(DateTimeUtils.isValidEpochMillis("1605094715000"));
  }

  @Test
  public void isValidEpochMillisTest_Invalid() {
    assertFalse(DateTimeUtils.isValidEpochMillis("1605094715"));
  }

  @Test
  public void isValidEpochSecondsTest() {
    assertTrue(DateTimeUtils.isValidEpochSeconds("1605094715"));
  }

  @Test
  public void isValidEpochSecondsTest_Invalid() {
    assertFalse(DateTimeUtils.isValidEpochSeconds("1605094715000"));
  }

  @Test
  public void parseInstantTestWithNull() {
    assertNotNull(DateTimeUtils.parseInstant(null));
  }

  @Test
  public void parseInstantTestWithEpochMillis() {
    assertEquals(DateTimeUtils.parseInstant("1605094715000"), Instant.ofEpochMilli(1605094715000l));
  }

  @Test
  public void parseInstantTestWithEpochSeconds() {
    assertEquals(DateTimeUtils.parseInstant("1605094715"), Instant.ofEpochMilli(1605094715l));
  }

  @Test
  public void parseInstantTestWithUTCTime() {
    assertEquals(DateTimeUtils.parseInstant("2020-11-10T14:24:35Z"),
        Instant.parse("2020-11-10T14:24:35Z"));
  }

  @Test
  public void parseInstantTestWithRelativeTime() {
    assertNotNull(DateTimeUtils.parseInstant("1s-ago"));
  }
}
