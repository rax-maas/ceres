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

package com.rackspace.ceres.app.entities;

import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

import java.util.Set;

@Table("metric_groups")
@Data
public class MetricGroup {
    @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
    String tenant;

    @PrimaryKeyColumn(value = "metric_group",
            /*
            Differs from other tables in order to allow efficient query of
            SELECT metric_group FROM metric_groups WHERE tenant = ?
             */
            type = PrimaryKeyType.CLUSTERED,
            ordinal = 1)
    String metricGroup;

    @Column(value = "metric_names")
    Set<String> metricNames;

    @Column(value = "updated_at")
    String updatedAt;
}
