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

package com.rackspace.ceres.app.services;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = {SeriesSetService.class})
@ActiveProfiles("test")
class SeriesSetServiceTest {

  @Autowired
  SeriesSetService seriesSetService;

  @Test
  void hash() {
    String result = seriesSetService.hash("cpu_idle", Map.of(
        "os", "linux",
        "deployment", "prod"
    ));
    assertThat(result).isEqualTo("r4oa9hFoLqxF3eAXYrLb6g");

    // vary one thing to confirm differing hash
    result = seriesSetService.hash("cpu_idle", Map.of(
        "os", "linux",
        // ...this tag value
        "deployment", "dev"
    ));
    assertThat(result).isEqualTo("tRhKuFybAMGyRdS4Cw9GPg");

    // vary one thing to confirm differing hash
    result = seriesSetService.hash(
        // ...this metric name
        "cpu_used", Map.of(
        "os", "linux",
        "deployment", "prod"
    ));
    assertThat(result).isEqualTo("EXx07xG1KTb2_SGR0JFQOQ");

    result = seriesSetService.hash("cpu_idle", Map.of(
        // ...this tag key
        "operating_system", "linux",
        "deployment", "prod"
    ));
    assertThat(result).isEqualTo("hInPoxD4-l_bpG4Vp9F9DA");

    result = seriesSetService.hash("cpu_idle", Map.of(
        // ...reordering of tags
        "deployment", "prod",
        "os", "linux"
    ));
    // produces same hash
    assertThat(result).isEqualTo("r4oa9hFoLqxF3eAXYrLb6g");
  }
}
