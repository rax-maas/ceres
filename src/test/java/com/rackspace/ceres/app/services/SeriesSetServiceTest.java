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

import com.rackspace.ceres.app.model.MetricNameAndTags;
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
  void buildSeriesSet() {
    final String result = seriesSetService.buildSeriesSet("name_here", Map.of(
        "os", "linux",
        "host", "server-1",
        "deployment", "prod"
    ));

    assertThat(result).isEqualTo("name_here,deployment=prod,host=server-1,os=linux");
  }

  @Test
  void expandSeriesSet() {
    final MetricNameAndTags result = seriesSetService
        .expandSeriesSet("name_here,deployment=prod,host=server-1,os=linux");

    assertThat(result).isNotNull();
    assertThat(result.getMetricName()).isEqualTo("name_here");
    assertThat(result.getTags()).containsOnly(
        Map.entry("deployment", "prod"),
        Map.entry("host", "server-1"),
        Map.entry("os", "linux")
    );
  }
}
