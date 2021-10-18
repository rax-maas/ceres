package com.rackspace.ceres.app.utils;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.model.RelativeTime;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DateTimeUtils {

  public static final String RELATIVE_TIME_PATTERN = "([0-9]+)(ms|s|m|h|d|w|n|y)-ago";
  public static final String EPOCH_MILLIS_PATTERN = "\\d{13,}";
  public static final String EPOCH_SECONDS_PATTERN = "\\d{1,12}";

  /**
   * Gets the absolute Instant instance for relativeTime.
   *
   * @param relativeTime
   * @return
   */
  public static Instant getAbsoluteTimeFromRelativeTime(String relativeTime) {
    Matcher match = Pattern.compile(RELATIVE_TIME_PATTERN).matcher(relativeTime);
    if (match.matches()) {
      return Instant.now()
          .minus(Integer.parseInt(match.group(1)), RelativeTime.valueOf(match.group(2)).getValue());
    } else {
      throw new IllegalArgumentException("Invalid relative time format");
    }
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
    return match.matches();
  }

  /**
   * Checks if the  time is valid epoch seconds.
   *
   * @param time
   * @return
   */
  public static boolean isValidEpochSeconds(String time) {
    Matcher match = Pattern.compile(DateTimeUtils.EPOCH_SECONDS_PATTERN).matcher(time);
    return match.matches();
  }

  /**
   * Gets the instance of Instant based on the format of argument.
   *
   * @param instant
   * @return
   */
  public static Instant parseInstant(String instant) {
    if (instant == null) {
      return Instant.now();
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
  
  public static Duration getGranularity(Duration duration, List<Granularity> granularities) {
    granularities = granularities.stream()
        .sorted(Comparator.comparingLong(value -> value.getTtl().toHours()))
        .collect(Collectors.toList());

    // Find the first granularity less or equal to the suggested width or return the last one
    Granularity granularity = granularities.stream()
        .filter(granularity1 -> duration.toMinutes() <= granularity1.getWidth().toMinutes())
        .findFirst()
        .orElse(granularities.get(granularities.size() - 1));
    return granularity.getWidth();
  }

  public static Duration getGranularity(Instant startTime, Instant endTime,
      List<Granularity> granularities) {
    Duration duration = Duration.between(startTime, endTime).truncatedTo(ChronoUnit.SECONDS);

    //Sorting the granularities
    granularities = granularities.stream()
        .sorted(Comparator.comparingLong(value -> value.getTtl().toHours()))
        .collect(Collectors.toList());

    Granularity granularity = granularities.stream()
        .filter(granularity1 -> duration.toHours() < granularity1.getTtl().toHours())
        .findFirst()
        .orElse(granularities.get(granularities.size() - 1));
    return granularity.getWidth();
  }

  public static String isoTimeUtcPlusSeconds(long seconds) {
    return isoTimeUtcPlusMilliSeconds(seconds * 1000);
  }

  public static String isoTimeUtcPlusMilliSeconds(long milliSeconds) {
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    Date now = new Date();
    now.setTime(now.getTime() + milliSeconds);
    return df.format(now);
  }
}
