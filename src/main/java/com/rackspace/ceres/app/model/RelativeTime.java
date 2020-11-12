package com.rackspace.ceres.app.model;

import java.time.temporal.ChronoUnit;

public enum RelativeTime {
  ms(ChronoUnit.MILLIS),
  s(ChronoUnit.SECONDS),
  m(ChronoUnit.MINUTES),
  h(ChronoUnit.HOURS),
  d(ChronoUnit.DAYS),
  w(ChronoUnit.WEEKS),
  n(ChronoUnit.MONTHS),
  y(ChronoUnit.YEARS);

  ChronoUnit value;

  RelativeTime(ChronoUnit unit) {
    this.value = unit;
  }

  public ChronoUnit getValue() {
    return value;
  }
}
