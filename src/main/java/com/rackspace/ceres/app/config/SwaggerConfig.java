/*
 * Copyright 2021 Rackspace US, Inc.
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

import java.util.ArrayList;
import java.util.List;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.builders.RequestParameterBuilder;
import springfox.documentation.schema.ScalarType;
import springfox.documentation.service.ParameterType;
import springfox.documentation.service.RequestParameter;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

  @Bean
  public Docket api() {
    return new Docket(DocumentationType.OAS_30)
        .select()
        .apis(RequestHandlerSelectors.any())
        .paths(PathSelectors.any())
        .build()
        .globalRequestParameters(addParameters());
  }

  /**
   * Add common parameters to all the requests headers.
   * @return
   */
  private List<RequestParameter> addParameters() {
    List<RequestParameter> requestParameters = new ArrayList<>();
    RequestParameter xTenantHeader = new RequestParameterBuilder().name("X-Tenant")
        .description("Tenant Id").required(true).in(
            ParameterType.HEADER).build();

    RequestParameter xAuthTokenHeader = new RequestParameterBuilder().name("X-Auth-Token")
        .description(
            "Either of X-Auth Token or X-Username and X-Password/X-Api-Key should be present")
        .in(ParameterType.HEADER).build();

    RequestParameter xUsernameHeader = new RequestParameterBuilder().name("X-Username")
        .description("This header is required when X-Auth-Token header"
            + "is not provided and it goes in combination with either X-Password or X-Api-Key headers")
        .in(ParameterType.HEADER).build();

    RequestParameter xPasswordHeader = new RequestParameterBuilder().name("X-Password")
        .description("Required header if X-Username is given and X-Api-Key is not specified")
        .in(ParameterType.HEADER).build();

    RequestParameter xApiKeyHeader = new RequestParameterBuilder().name("X-Api-Key")
        .description("Required header if X-Username is given and X-Password is not specified")
        .in(ParameterType.HEADER).build();

    requestParameters.add(xTenantHeader);
    requestParameters.add(xAuthTokenHeader);
    requestParameters.add(xUsernameHeader);
    requestParameters.add(xPasswordHeader);
    requestParameters.add(xApiKeyHeader);

    return requestParameters;
  }
}