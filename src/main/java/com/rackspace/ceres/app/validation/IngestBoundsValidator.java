/*
 * Copyright 2021 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.ceres.app.validation;

import static java.time.temporal.ChronoUnit.SECONDS;

import com.rackspace.ceres.app.config.AppProperties;
import java.time.Duration;
import java.time.Instant;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("ingest")
public class IngestBoundsValidator implements ConstraintValidator<IngestBounds, Instant> {

  private final AppProperties appProperties;
  private Duration startDuration;
  private Duration endDuration;

  public IngestBoundsValidator(AppProperties appProperties) {
    this.appProperties = appProperties;
  }

  @Override
  public void initialize(IngestBounds constraintAnnotation) {
    startDuration = appProperties.getIngestStartTime();
    endDuration = appProperties.getIngestEndTime();
  }

  @Override
  public boolean isValid(Instant value, ConstraintValidatorContext context) {
    Instant currentInstant = Instant.now();
    var startPeriod = currentInstant.minus(startDuration.toSeconds(), SECONDS);
    var endPeriod = currentInstant.plus(endDuration.toSeconds(), SECONDS);
    return value.isAfter(startPeriod) && value.isBefore(endPeriod);
  }
}
