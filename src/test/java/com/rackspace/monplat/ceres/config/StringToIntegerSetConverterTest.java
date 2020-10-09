/*
 * Copyright 2020 Rackspace US, Inc.
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

package com.rackspace.monplat.ceres.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StringToIntegerSetConverterTest {

  @Test
  void combo() {
    final StringToIntegerSetConverter converter = new StringToIntegerSetConverter();

    final IntegerSet results = converter.convert("1,5-8,12,15-18");

    assertThat(results).containsExactly(
        1, 5, 6, 7, 8, 12, 15, 16, 17, 18
    );
  }

  @Test
  void nullInput() {
    final StringToIntegerSetConverter converter = new StringToIntegerSetConverter();

    final IntegerSet results = converter.convert(null);

    assertThat(results).isNotNull();
    assertThat(results).hasSize(0);
  }

  @Test
  void blankInput() {
    final StringToIntegerSetConverter converter = new StringToIntegerSetConverter();

    final IntegerSet results = converter.convert("");

    assertThat(results).isNotNull();
    assertThat(results).hasSize(0);
  }
}
