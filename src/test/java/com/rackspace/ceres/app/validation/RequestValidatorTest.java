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
 *
 */

package com.rackspace.ceres.app.validation;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = RequestValidator.class)
@ActiveProfiles("test")
public class RequestValidatorTest {

  @Autowired
  private RequestValidator requestValidator;


  @Test
  public void validate_AllEmpty() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> requestValidator
        .validate("", "", ""));
  }

  @Test
  public void validate_AllNull() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> requestValidator
        .validate(null, null, null));
  }

  @Test
  public void validate_AllNotBlank() {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    final String device = RandomStringUtils.randomAlphabetic(5);

    Assertions.assertThrows(IllegalArgumentException.class, () -> requestValidator
        .validate(metricName, metricGroup, device));
  }

  @Test
  public void validate_Success() {
    final String metricName = RandomStringUtils.randomAlphabetic(5);

    Assertions.assertDoesNotThrow(() -> requestValidator
        .validate(metricName, null, ""));
  }
}
