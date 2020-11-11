package com.rackspace.ceres.app.utils;

import com.rackspace.ceres.app.model.RelativeTime;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTimeUtils {

  public static final String RELATIVE_TIME_PATTERN = "([0-9]+)(ms|s|m|h|d|w|n|y)-ago";
  public static final String EPOCH_MILLIS_PATTERN = "^\\d{13,}$";
  public static final String EPOCH_SECONDS_PATTERN = "^\\d{1,12}$";

  /**
   * Gets the absolute Instant instance for relativeTime.
   *
   * @param relativeTime
   * @return
   */
  public static Instant getAbsoluteTimeFromRelativeTime(String relativeTime) {
    Matcher match = Pattern.compile(RELATIVE_TIME_PATTERN).matcher(relativeTime);
    int timeValue = 0;
    String timeUnit = null;
    if (match.matches()) {
      timeValue = Integer.parseInt(match.group(1));
      timeUnit = match.group(2);
    }
    if(timeUnit!=null && timeValue!=0)
      return Instant.now().minus(timeValue, RelativeTime.valueOf(timeUnit).getValue());
    else
      throw new IllegalArgumentException("Invalid relative time format");
  }

  /**
   * Checks if the string time is valid Instant in UTC.
   *
   * @param time
   * @return
   */
  public static boolean isValidInstantInstance(String time) {
    try {
      Instant.parse(time);
      return true;
    } catch (DateTimeParseException dateTimeParseException) {
      return false;
    }
  }

  /**
   * Checks if the  time is valid epoch milli seconds.
   *
   * @param time
   * @return
   */
  public static boolean isValidEpochMillis(String time) {
    Matcher match = Pattern.compile(DateTimeUtils.EPOCH_MILLIS_PATTERN).matcher(time);
    if(match.matches()) {
      return true;
    }
    return false;
  }

  /**
   * Checks if the  time is valid epoch seconds.
   *
   * @param time
   * @return
   */
  public static boolean isValidEpochSeconds(String time) {
    Matcher match = Pattern.compile(DateTimeUtils.EPOCH_SECONDS_PATTERN).matcher(time);
    if(match.matches()) {
      return true;
    }
    return false;
  }

  /**
   * Gets the instance of Instant based on the format of argument.
   *
   * @param instant
   * @return
   */
  public static Instant parseInstant(String instant) {
    if(instant==null) {
      return  Instant.now();
    }
    if (isValidInstantInstance(instant)) {
      return Instant.parse(instant);
    } else if (isValidEpochMillis(instant)) {
      return Instant.ofEpochMilli(Long.parseLong(instant));
    } else if (isValidEpochSeconds(instant)) {
      return Instant.ofEpochSecond(Long.parseLong(instant));
    } else {
      return getAbsoluteTimeFromRelativeTime(instant);
    }
  }
}
