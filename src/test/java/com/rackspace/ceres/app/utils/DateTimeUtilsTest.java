package com.rackspace.ceres.app.utils;

import com.rackspace.ceres.app.config.DownsampleProperties.Granularity;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    assertThat(Duration.between(DateTimeUtils.parseInstant(null), Instant.now()).getSeconds())
        .isLessThanOrEqualTo(1);
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

  @Test
  public void getGranularityTest() {
    Granularity granularity1 = new Granularity();
    granularity1.setWidth(Duration.ofMinutes(5));
    granularity1.setTtl(Duration.ofDays(14));

    Granularity granularity2 = new Granularity();
    granularity2.setWidth(Duration.ofHours(1));
    granularity2.setTtl(Duration.ofDays(365));

    Granularity granularity3 = new Granularity();
    granularity3.setWidth(Duration.ofHours(2));
    granularity3.setTtl(Duration.ofDays(500));

    List<Granularity> granularityList = List.of(granularity1, granularity2, granularity3);

    assertThat(DateTimeUtils
        .getGranularity(Instant.now(), Instant.now().plus(1, ChronoUnit.HOURS), granularityList))
        .isEqualTo(Duration.ofMinutes(5));

    assertThat(DateTimeUtils
        .getGranularity(Instant.now(), Instant.now().plus(15, ChronoUnit.DAYS), granularityList))
        .isEqualTo(Duration.ofHours(1));

    assertThat(DateTimeUtils
        .getGranularity(Instant.now(), Instant.now().plus(400, ChronoUnit.DAYS), granularityList))
        .isEqualTo(Duration.ofHours(2));
  }

  @Test
  public void filterGroupGranularitiesTest() {
    Granularity granularity1 = new Granularity();
    granularity1.setWidth(Duration.ofMinutes(5));
    granularity1.setPartitionWidth(Duration.ofMinutes(5));
    granularity1.setTtl(Duration.ofDays(14));

    Granularity granularity2 = new Granularity();
    granularity2.setWidth(Duration.ofHours(1));
    granularity2.setPartitionWidth(Duration.ofHours(2));
    granularity2.setTtl(Duration.ofDays(365));

    Granularity granularity3 = new Granularity();
    granularity3.setWidth(Duration.ofHours(2));
    granularity3.setPartitionWidth(Duration.ofHours(2));
    granularity3.setTtl(Duration.ofDays(500));

    Granularity granularity4 = new Granularity();
    granularity4.setWidth(Duration.ofHours(3));
    granularity4.setPartitionWidth(Duration.ofHours(3));
    granularity4.setTtl(Duration.ofDays(500));

    List<Granularity> granularityList = List.of(granularity1, granularity2, granularity3, granularity4);

    assertThat(hasNoMatch(DateTimeUtils.filterGroupGranularities("PT5M", granularityList), "PT5M")).isEqualTo(Boolean.FALSE);
    assertThat(hasNoMatch(DateTimeUtils.filterGroupGranularities("PT2H", granularityList), "PT2H")).isEqualTo(Boolean.FALSE);
    assertThat(hasNoMatch(DateTimeUtils.filterGroupGranularities("PT3H", granularityList), "PT3H")).isEqualTo(Boolean.FALSE);
  }

  @Test
  public void getPartitionWidthsTest() {
    Granularity granularity1 = new Granularity();
    granularity1.setWidth(Duration.ofMinutes(5));
    granularity1.setPartitionWidth(Duration.ofMinutes(5));
    granularity1.setTtl(Duration.ofDays(14));

    Granularity granularity2 = new Granularity();
    granularity2.setWidth(Duration.ofHours(1));
    granularity2.setPartitionWidth(Duration.ofHours(2));
    granularity2.setTtl(Duration.ofDays(365));

    Granularity granularity3 = new Granularity();
    granularity3.setWidth(Duration.ofHours(2));
    granularity3.setPartitionWidth(Duration.ofHours(2));
    granularity3.setTtl(Duration.ofDays(500));

    Granularity granularity4 = new Granularity();
    granularity4.setWidth(Duration.ofHours(3));
    granularity4.setPartitionWidth(Duration.ofHours(3));
    granularity4.setTtl(Duration.ofDays(500));

    List<Granularity> granularityList = List.of(granularity4, granularity1, granularity3, granularity2);
    List<String> widths = DateTimeUtils.getPartitionWidths(granularityList);
    assertThat(widths.equals(List.of("PT5M", "PT2H", "PT3H"))).isEqualTo(Boolean.TRUE);
  }

  @Test
  public void getDurationLowerThan() {
    Granularity granularity1 = new Granularity();
    granularity1.setWidth(Duration.ofMinutes(5));
    granularity1.setPartitionWidth(Duration.ofMinutes(5));
    granularity1.setTtl(Duration.ofDays(14));

    Granularity granularity2 = new Granularity();
    granularity2.setWidth(Duration.ofHours(1));
    granularity2.setPartitionWidth(Duration.ofHours(2));
    granularity2.setTtl(Duration.ofDays(365));

    Granularity granularity3 = new Granularity();
    granularity3.setWidth(Duration.ofHours(2));
    granularity3.setPartitionWidth(Duration.ofHours(2));
    granularity3.setTtl(Duration.ofDays(500));

    Granularity granularity4 = new Granularity();
    granularity4.setWidth(Duration.ofHours(3));
    granularity4.setPartitionWidth(Duration.ofHours(3));
    granularity4.setTtl(Duration.ofDays(500));

    List<Granularity> granularityList = List.of(granularity4, granularity1, granularity3, granularity2);
    assertThat(DateTimeUtils.getLowerGranularity(granularityList, Duration.ofHours(4)).toString().equals("PT3H")).isEqualTo(Boolean.TRUE);
    assertThat(DateTimeUtils.getLowerGranularity(granularityList, Duration.ofHours(3)).toString().equals("PT2H")).isEqualTo(Boolean.TRUE);
    assertThat(DateTimeUtils.getLowerGranularity(granularityList, Duration.ofHours(2)).toString().equals("PT1H")).isEqualTo(Boolean.TRUE);
    assertThat(DateTimeUtils.getLowerGranularity(granularityList, Duration.ofHours(1)).toString().equals("PT5M")).isEqualTo(Boolean.TRUE);
    assertThat(DateTimeUtils.getLowerGranularity(granularityList, Duration.ofMinutes(5)).toString().equals("PT0S")).isEqualTo(Boolean.TRUE);
    assertThat(DateTimeUtils.getLowerGranularity(granularityList, Duration.ofMinutes(0)).toString().equals("PT0S")).isEqualTo(Boolean.TRUE);
  }

  @Test
  public void getPartitionWidthTest() {
    Granularity granularity1 = new Granularity();
    granularity1.setWidth(Duration.ofMinutes(5));
    granularity1.setPartitionWidth(Duration.ofMinutes(5));
    granularity1.setTtl(Duration.ofDays(14));

    Granularity granularity2 = new Granularity();
    granularity2.setWidth(Duration.ofHours(1));
    granularity2.setPartitionWidth(Duration.ofHours(2));
    granularity2.setTtl(Duration.ofDays(365));

    Granularity granularity3 = new Granularity();
    granularity3.setWidth(Duration.ofHours(2));
    granularity3.setPartitionWidth(Duration.ofHours(2));
    granularity3.setTtl(Duration.ofDays(500));

    Granularity granularity4 = new Granularity();
    granularity4.setWidth(Duration.ofHours(3));
    granularity4.setPartitionWidth(Duration.ofHours(3));
    granularity4.setTtl(Duration.ofDays(500));

    List<Granularity> granularityList = List.of(granularity4, granularity1, granularity3, granularity2);
    assertThat(DateTimeUtils.getPartitionWidth(granularityList, Duration.ofMinutes(5))).isEqualTo(Duration.ofMinutes(5));
    assertThat(DateTimeUtils.getPartitionWidth(granularityList, Duration.ofHours(1))).isEqualTo(Duration.ofHours(2));
    assertThat(DateTimeUtils.getPartitionWidth(granularityList, Duration.ofHours(2))).isEqualTo(Duration.ofHours(2));
  }

  private boolean hasNoMatch(final List<Granularity> granularities, final String width){
    return granularities.stream().anyMatch(o -> o.getPartitionWidth().compareTo(Duration.parse(width)) != 0);
  }
}
