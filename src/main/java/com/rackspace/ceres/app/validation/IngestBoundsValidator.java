package com.rackspace.ceres.app.validation;

import com.rackspace.ceres.app.config.AppProperties;
import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.time.*;
import java.time.temporal.ChronoUnit;

@Log4j2
@Component
@Profile("ingest")
public class IngestBoundsValidator implements ConstraintValidator<IngestBounds, Instant> {

  private final AppProperties appProperties;

  public IngestBoundsValidator(AppProperties appProperties) {
    this.appProperties = appProperties;
  }

  @Override
  public boolean isValid(Instant value, ConstraintValidatorContext context) {
    log.info("Field Value - {}", value);
    value = value.atZone(ZoneId.of("UTC")).toInstant();
    return !(value.isBefore(getStartTime()) || value.isAfter(getEndTime()));
  }

  /**
   * This method calculates the start ingested metrics time, in case no value is provided the
   * default value is set to 1 week in the past.
   *
   * @return - {@code Instant}
   */
  private Instant getStartTime() {
    int startParams;
    if (appProperties.getIngestStartTime() == null || appProperties.getIngestStartTime()
        .isEmpty()) {
      startParams = 7;
    } else {
      String extractStartTime = appProperties.getIngestStartTime().replaceAll("[^0-9]", "");
      startParams = Integer.parseInt(extractStartTime);
    }

    LocalDateTime dateTime = LocalDate.now().atStartOfDay().minus(startParams, ChronoUnit.DAYS);
    Instant startTimeStamp = ZonedDateTime.of(dateTime, ZoneId.of("UTC")).toInstant();
    log.info("Start Time Stamp - {}", startTimeStamp);
    return startTimeStamp;
  }

  /**
   * This method calculates the end ingested metrics time, in case no value is provided the default
   * value is set to 1 day in the future.
   *
   * @return - {@code Instant}
   */
  private Instant getEndTime() {
    int endParams;
    if (appProperties.getIngestEndTime() == null || appProperties.getIngestEndTime().isEmpty()) {
      endParams = 1;
    } else {
      String extractEndTime = appProperties.getIngestEndTime().replaceAll("[^0-9]", "");
      endParams = Integer.parseInt(extractEndTime);
    }
    LocalDateTime dateTime = LocalDate.now().atTime(LocalTime.MAX).plus(endParams, ChronoUnit.DAYS);
    Instant endTimeStamp = ZonedDateTime.of(dateTime, ZoneId.of("UTC")).toInstant();
    log.info("End Time Stamp - {}", endTimeStamp);
    return endTimeStamp;
  }
}
