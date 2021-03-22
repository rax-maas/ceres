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
public class InBetweenValidator implements ConstraintValidator<InBetween, Instant> {

    private final AppProperties appProperties;

    public InBetweenValidator(AppProperties appProperties) {
        this.appProperties = appProperties;
    }

    @Override
    public boolean isValid(Instant value, ConstraintValidatorContext context) {
        log.info("Field Value - {}", value);
        value = value.atZone(ZoneId.of("UTC")).toInstant();
        return !(value.isBefore(getStartTime()) || value.isAfter(getEndTime()));
    }

    private Instant getStartTime() {
        int startParams = Integer.parseInt(appProperties.getIngestStartTime());
        LocalDateTime dateTime = LocalDate.now().atStartOfDay().minus(startParams, ChronoUnit.DAYS);
        Instant startTimeStamp = ZonedDateTime.of(dateTime, ZoneId.of("UTC")).toInstant();
        log.info("Start Time Stamp - {}", startTimeStamp);
        return startTimeStamp;
    }

    private Instant getEndTime() {
        int endParams = Integer.parseInt(appProperties.getIngestEndTime());
        LocalDateTime dateTime = LocalDate.now().atTime(LocalTime.MAX).plus(endParams, ChronoUnit.DAYS);
        Instant endTimeStamp = ZonedDateTime.of(dateTime, ZoneId.of("UTC")).toInstant();
        log.info("End Time Stamp - {}", endTimeStamp);
        return endTimeStamp;
    }
}
