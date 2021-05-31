package com.rackspace.ceres.app.validation;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;

import com.rackspace.ceres.app.config.AppProperties;
import java.time.Duration;
import java.time.Instant;
import org.junit.Test;

public class IngestBoundsValidatorTest {

  @Test
  public void testIngestBounds() {
    IngestBoundsValidator validator = new IngestBoundsValidator(getAppProperties());
    validator.initialize(any());
    Instant value = Instant.now();
    var result = validator.isValid(value, any());
    assertTrue(result);
  }

  @Test
  public void testIngestBoundsWithOlderIngest() {
    IngestBoundsValidator validator = new IngestBoundsValidator(getAppProperties());
    validator.initialize(null);
    Instant value = Instant.now().minus(8, DAYS);
    var result = validator.isValid(value, any());
    assertFalse(result);
  }

  @Test
  public void testIngestBoundsWithDiffBoundaryUnits() {
    AppProperties properties = new AppProperties();
    properties.setIngestStartTime(Duration.ofHours(12));
    properties.setIngestEndTime(Duration.ofDays(1));
    IngestBoundsValidator validator = new IngestBoundsValidator(getAppProperties());
    validator.initialize(null);
    Instant value = Instant.now().minus(11, HOURS);
    var result = validator.isValid(value, any());
    assertTrue(result);
  }

  private AppProperties getAppProperties() {
    AppProperties properties = new AppProperties();
    properties.setIngestStartTime(Duration.ofDays(7));
    properties.setIngestEndTime(Duration.ofDays(1));
    return properties;
  }
}
