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

import com.rackspace.ceres.app.config.AppProperties;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.PendingDownsampleSet;
import com.rackspace.ceres.app.utils.DateTimeUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.cassandra.core.cql.ReactiveCqlTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@SpringBootTest(classes = {
    RedisAutoConfiguration.class
})
@EnableConfigurationProperties({DownsampleProperties.class, AppProperties.class})
@ActiveProfiles(profiles = {"test"})
class DownsampleTrackingServiceTest {
  @Autowired
  DownsampleProperties properties;
  @Autowired
  AppProperties appProperties;
  @MockBean
  ReactiveCqlTemplate cqlTemplate;
  @MockBean
  ReactiveStringRedisTemplate redisTemplate;
  @MockBean
  RedisScript<String> redisGetJob;

  @Test
  public void isTimeslotDue() {
    Long t1 = DateTimeUtils.normalizedTimeslot(Instant.now(), "PT5M");
    DownsampleTrackingService downsampleTrackingService =
        new DownsampleTrackingService(redisTemplate, redisGetJob, properties, appProperties, cqlTemplate);
    Assertions.assertFalse(downsampleTrackingService.isTimeslotDue(t1.toString(), "PT5M"));
    Long t2 = DateTimeUtils.normalizedTimeslot(Instant.now().minus(10, ChronoUnit.MINUTES), "PT5M");
    Assertions.assertTrue(downsampleTrackingService.isTimeslotDue(t2.toString(), "PT5M"));
  }

  @Test
  public void buildPending() {
    Long timeslot = DateTimeUtils.normalizedTimeslot(Instant.now(), "PT15M");
    String setHash = String.format("%s|%s", "t-1", "my-hash-string");
    PendingDownsampleSet set = DownsampleTrackingService.buildPending(timeslot, setHash);
    Assertions.assertEquals(Instant.ofEpochSecond(timeslot), set.getTimeSlot());
    Assertions.assertEquals("my-hash-string", set.getSeriesSetHash());
    Assertions.assertEquals("t-1", set.getTenant());
  }
}
