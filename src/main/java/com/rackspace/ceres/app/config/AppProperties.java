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

package com.rackspace.ceres.app.config;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.validation.constraints.NotNull;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.convert.DurationUnit;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("ceres")
@Component
@Data
@Validated
public class AppProperties {
  @NotNull
  @DurationUnit(ChronoUnit.SECONDS)
  Duration rawTtl = Duration.ofHours(6);

  /**
   * Identifies the tenant for ingest and query API calls. For ingest this header is optional
   * and instead <code>tenant-tag</code> will be used or the configured <code>default-tenant</code>
   * as a fallback.
   */
  String tenantHeader = "X-Tenant";
  /**
   * When the tenant header is not present during ingest, a tag with this key will be used. If the tag
   * is present, it is removed from the tags of the metric since tenant is stored as a distinct
   * column.
   */
  String tenantTag = "tenant";
  /**
   * When tenant header and tag is not present during ingest, then this value will be used as
   * the default.
   */
  String defaultTenant = "default";
}
