/*
 * Copyright 2021 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.ceres.app.entities;

import com.rackspace.ceres.app.model.SuggestType;
import lombok.Data;
import org.springframework.data.cassandra.core.cql.PrimaryKeyType;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.cassandra.core.mapping.CassandraType.Name;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.Table;

@Table("tags_data")
@Data
public class TagsData {

  @PrimaryKeyColumn(type = PrimaryKeyType.PARTITIONED, ordinal = 0)
  String tenant;

  @PrimaryKeyColumn(value = "type", type = PrimaryKeyType.PARTITIONED, ordinal = 1)
  @CassandraType(type = Name.TEXT)
  SuggestType type;

  @PrimaryKeyColumn(value = "data", type = PrimaryKeyType.CLUSTERED, ordinal = 2)
  String data;
}
