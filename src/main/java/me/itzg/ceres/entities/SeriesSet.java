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

package me.itzg.ceres.entities;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Data
@Table("series_sets")
public class SeriesSet {

  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  String tenant;

  @PrimaryKeyColumn(value = "metric_name", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  String metricName;

  @PrimaryKeyColumn(value = "tag_key", type = PrimaryKeyType.CLUSTERED, ordinal = 2)
  String tagKey;

  @PrimaryKeyColumn(value = "tag_value", type = PrimaryKeyType.CLUSTERED, ordinal = 3)
  String tagValue;

  @PrimaryKeyColumn(value = "series_set", type = PrimaryKeyType.CLUSTERED, ordinal = 4)
  String seriesSet;
}
