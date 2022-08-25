/*
 * Copyright 2022 Rackspace US, Inc.
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

package com.rackspace.ceres.app;

import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

public class ESContainerSetup extends ElasticsearchContainer {

  private static final String ELASTIC_SEARCH_DOCKER = "elasticsearch:7.6.2";

  private static final String CLUSTER_NAME = "cluster.name";

  private static final String ELASTIC_SEARCH = "elasticsearch";

  public ESContainerSetup() {
    super(DockerImageName.parse(ELASTIC_SEARCH_DOCKER)
        .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch"));
    this.addFixedExposedPort(9200, 9200);
    this.addFixedExposedPort(9300, 9300);
    this.addEnv(CLUSTER_NAME, ELASTIC_SEARCH);
  }
}

