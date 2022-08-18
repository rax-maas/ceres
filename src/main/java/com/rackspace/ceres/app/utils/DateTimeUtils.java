package com.rackspace.ceres.app.utils;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import com.rackspace.ceres.app.downsample.TemporalNormalizer;
import com.rackspace.ceres.app.model.RelativeTime;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DateTimeUtils {

  public static final String RELATIVE_TIME_PATTERN = "([0-9]+)(ms|s|m|h|d|w|n|y)-ago";
  public static final String EPOCH_MILLIS_PATTERN = "\\d{13,}";
  public static final String EPOCH_SECONDS_PATTERN = "\\d{1,12}";

  /**
   * Gets the absolute Instant instance for relativeTime.
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
   * Checks if the  time is valid epoch milliseconds.
   */
  public static boolean isValidEpochMillis(String time) {
    Matcher match = Pattern.compile(DateTimeUtils.EPOCH_MILLIS_PATTERN).matcher(time);
    return match.matches();
  }

  /**
   * Checks if the  time is valid epoch seconds.
   */
  public static boolean isValidEpochSeconds(String time) {
    Matcher match = Pattern.compile(DateTimeUtils.EPOCH_SECONDS_PATTERN).matcher(time);
    return match.matches();
  }

  /**
   * Gets the instance of Instant based on the format of argument.
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

  public static List<Granularity> filterGroupGranularities(String group, List<Granularity> granularities) {
    Duration width = Duration.parse(group);
    return granularities.stream()
            .filter(g -> g.getPartitionWidth().compareTo(width) == 0)
            .collect(Collectors.toList());
  }

  public static Duration getLowerGranularity(List<Granularity> granularities, Duration width) {
    List<Duration> durationsLowerThan =
        granularities.stream()
            .map(Granularity::getWidth).sorted()
            .filter(g -> g.getSeconds() < width.getSeconds())
            .collect(Collectors.toList());
    if (durationsLowerThan.isEmpty()) {
      return Duration.parse("PT0S");
    } else {
      return durationsLowerThan.get(durationsLowerThan.size() - 1);
    }
  }

  public static Duration getPartitionWidth(List<Granularity> granularities, Duration width) {
    return granularities.stream()
        .filter(granularity -> granularity.getWidth().equals(width))
        .findFirst()
        .orElseThrow()
        .getPartitionWidth();
  }

  public static List<String> getPartitionWidths(List<Granularity> granularities) {
    return granularities.stream()
            .map(Granularity::getPartitionWidth).sorted()
            .collect(Collectors.toList())
            .stream().map(Duration::toString)
            .distinct()
            .collect(Collectors.toList());
  }

  public static long nowEpochSeconds() {
    return Instant.now().getEpochSecond();
  }

  public static LocalDateTime epochToLocalDateTime(long epochSeconds) {
    return Instant.ofEpochSecond(epochSeconds).atZone(ZoneId.systemDefault()).toLocalDateTime();
  }

  public static Long normalizedTimeslot(Instant timestamp, String group) {
    return timestamp.with(new TemporalNormalizer(Duration.parse(group))).getEpochSecond();
  }

  public static Instant normalize(Instant timestamp, String group) {
    return timestamp.with(new TemporalNormalizer(Duration.parse(group)));
  }

  public static int randomDelay(Long maxInterval) {
      int minSchedulingInterval = 1;
      return Math.min(new Random().nextInt(maxInterval.intValue()) + minSchedulingInterval, maxInterval.intValue());
  }

  public static boolean isLowerGranularityRaw(Duration lowerWidth) {
    return lowerWidth.toString().equals("PT0S");
  }
}
