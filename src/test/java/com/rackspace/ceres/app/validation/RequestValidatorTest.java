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

import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles("test")
public class RequestValidatorTest {

  @Test
  public void validateMetricNameAndMetricGroupTest_MetricName() {
    final String metricName = RandomStringUtils.randomAlphabetic(5);

    Assertions.assertDoesNotThrow(() -> RequestValidator
        .validateMetricNameAndGroup(metricName, ""));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_Both_Non_Empty() {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);

    Assertions.assertThrows(IllegalArgumentException.class, () -> RequestValidator
        .validateMetricNameAndGroup(metricName, metricGroup));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_Both_Empty() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> RequestValidator
        .validateMetricNameAndGroup("", ""));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_Null_MetricName() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> RequestValidator
        .validateMetricNameAndGroup(null, ""));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_Null_MetricGroup() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> RequestValidator
        .validateMetricNameAndGroup("", null));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_Both_Null() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> RequestValidator
        .validateMetricNameAndGroup(null, null));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_WithMetricNameAndEmptyMetricGroup() {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    Assertions.assertDoesNotThrow(() -> RequestValidator
        .validateMetricNameAndGroup(metricName, ""));
  }

  @Test
  public void validateMetricNameAndMetricGroupTest_WithMetricNameAndNullMetricGroup() {
    final String metricName = RandomStringUtils.randomAlphabetic(5);
    Assertions.assertDoesNotThrow(() -> RequestValidator
        .validateMetricNameAndGroup(metricName, null));
  }

  @Test
  public void validateMetricGroupAndTags_WithMetricGroupPresentAndTagsNull() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Assertions.assertDoesNotThrow(() -> RequestValidator
        .validateMetricGroupAndTags(metricGroup, null));
  }

  @Test
  public void validateMetricGroupAndTags_WithMetricGroupPresentAndTagsPresent() {
    final String metricGroup = RandomStringUtils.randomAlphabetic(5);
    Assertions.assertThrows(IllegalArgumentException.class, () -> RequestValidator
        .validateMetricGroupAndTags(metricGroup, List.of("os=linux")));
  }
}
