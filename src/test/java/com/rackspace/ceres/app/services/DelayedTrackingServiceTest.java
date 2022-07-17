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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.test.context.ActiveProfiles;

import java.time.Instant;

@SpringBootTest(classes = {
    RedisAutoConfiguration.class
})
@EnableConfigurationProperties({DownsampleProperties.class, AppProperties.class})
@ActiveProfiles(profiles = {"test"})
class DelayedTrackingServiceTest {
  @Autowired
  DownsampleProperties properties;
  @Autowired
  AppProperties appProperties;
  @MockBean
  ReactiveStringRedisTemplate redisTemplate;
  @MockBean
  RedisScript<String> redisGetDelayedJob;

  @Test
  public void isNotInProgress() {
    DelayedTrackingService delayedTrackingService =
        new DelayedTrackingService(redisTemplate, properties, appProperties, redisGetDelayedJob, null);
    String timeslotInProgress = String.format("%d|t-1|my-little-hash|in-progress", Instant.now().getEpochSecond());
    Assertions.assertFalse(delayedTrackingService.isNotInProgress(timeslotInProgress, 1, "PT15M"));
    String timeslotNotInProgress = String.format("%d|t-1|my-little-hash", Instant.now().getEpochSecond());
    Assertions.assertTrue(delayedTrackingService.isNotInProgress(timeslotNotInProgress, 1, "PT15M"));
   }

  @Test
  public void isInProgressHanging() {
    DelayedTrackingService delayedTrackingService =
        new DelayedTrackingService(redisTemplate, properties, appProperties, redisGetDelayedJob, null);
    String timeslotHangingInProgress =
        String.format("%d|t-1|my-little-hash|in-progress",
            Instant.now().minusSeconds(appProperties.getIngestStartTime().getSeconds() + 1).getEpochSecond());
    Assertions.assertTrue(delayedTrackingService.isNotInProgress(timeslotHangingInProgress, 1, "PT15M"));
  }

  @Test
  public void isInProgress() {
    DelayedTrackingService delayedTrackingService =
        new DelayedTrackingService(redisTemplate, properties, appProperties, redisGetDelayedJob, null);
    String timeslotInProgress = String.format("%d|t-1|my-little-hash|in-progress", Instant.now().getEpochSecond());
    Assertions.assertTrue(delayedTrackingService.isInProgress(timeslotInProgress));
    String timeslotNotInProgress = String.format("%d|t-1|my-little-hash", Instant.now().getEpochSecond());
    Assertions.assertFalse(delayedTrackingService.isInProgress(timeslotNotInProgress));
  }

  @Test
  public void buildPendingSet() {
    String timeslot = "1655868939|t-1|my-little-hash";
    PendingDownsampleSet set = DelayedTrackingService.buildDownsampleSet(1, "PT15M", timeslot);
    Assertions.assertEquals(Instant.ofEpochSecond(1655868939), set.getTimeSlot());
    Assertions.assertEquals("my-little-hash", set.getSeriesSetHash());
    Assertions.assertEquals("t-1", set.getTenant());
  }

  @Test
  public void encodeDelayedTimeslot() {
    String timeslot = "1655868939|t-1|my-little-hash";
    PendingDownsampleSet set = DelayedTrackingService.buildDownsampleSet(1, "PT15M", timeslot);
    Assertions.assertEquals(Instant.ofEpochSecond(1655868939), set.getTimeSlot());
    Assertions.assertEquals("my-little-hash", set.getSeriesSetHash());
    Assertions.assertEquals("t-1", set.getTenant());
    Assertions.assertEquals(timeslot, DelayedTrackingService.encodeDelayedTimeslot(set));
  }
}
