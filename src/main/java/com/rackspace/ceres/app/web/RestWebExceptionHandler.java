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

package com.rackspace.ceres.app.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.TypeMismatchException;
import org.springframework.boot.autoconfigure.web.WebProperties;
import org.springframework.boot.autoconfigure.web.reactive.error.AbstractErrorWebExceptionHandler;
import org.springframework.boot.web.error.ErrorAttributeOptions;
import org.springframework.boot.web.error.ErrorAttributeOptions.Include;
import org.springframework.boot.web.reactive.error.ErrorAttributes;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.ServerCodecConfigurer;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.server.*;
import org.springframework.web.server.ServerWebInputException;
import reactor.core.publisher.Mono;

import java.util.Map;

@Slf4j
@Component
@Order(-2)//So that our exception handler gets picked before DefaultErrorWebExceptionHandler
public class RestWebExceptionHandler extends
    AbstractErrorWebExceptionHandler {

  public RestWebExceptionHandler(
      ErrorAttributes errorAttributes,
      WebProperties.Resources resourceProperties,
      ApplicationContext applicationContext,
      ServerCodecConfigurer serverCodecConfigurer) {
    super(errorAttributes, resourceProperties, applicationContext);
    this.setMessageWriters(serverCodecConfigurer.getWriters());
  }

  @Override
  protected RouterFunction<ServerResponse> getRoutingFunction(
      ErrorAttributes errorAttributes) {
    return RouterFunctions.route(RequestPredicates.all(), this::renderErrorResponse);
  }

  /**
   * Renders the error response.
   *
   * @param serverRequest
   * @return
   */
  private Mono<ServerResponse> renderErrorResponse(ServerRequest serverRequest) {
    Map<String, Object> body = getErrorAttributes(serverRequest, ErrorAttributeOptions.of(
        Include.EXCEPTION, Include.MESSAGE, Include.STACK_TRACE));
    String exceptionClass = (String) body.get("exception");
    logErrorMessage(serverRequest, (String) body.get("trace"), exceptionClass);
    body.remove("trace");
    return respondWith(body, exceptionClass);
  }

  /**
   * Responds with error response based on the exception class present in errorAttributes.
   *
   * @param body
   * @param exceptionClass
   * @return
   */
  private Mono<ServerResponse> respondWith(Map<String, Object> body, String exceptionClass) {
    if (exceptionClass.equals(IllegalArgumentException.class.getName()) || exceptionClass
        .equals(ServerWebInputException.class.getName()) || exceptionClass.equals(
        TypeMismatchException.class.getName())) {
      return respondWithBadRequest(body);
    }

    body.put("message", "Service encountered an unexpected "
        + "condition which prevented it from fulfilling the request.");
    return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(
        body));
  }

  /**
   * Responds with bad request server response.
   *
   * @param body
   * @return
   */
  private Mono<ServerResponse> respondWithBadRequest(Map<String, Object> body) {
    body.remove("error");
    body.put("status", HttpStatus.BAD_REQUEST.value());
    return ServerResponse.status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(body));
  }

  /**
   * Logs the error message with stack trace.
   *
   * @param serverRequest
   * @param stackTrace
   */
  private void logErrorMessage(ServerRequest serverRequest, String stackTrace, String exceptionClass) {
    if (exceptionClass.equals(ServerWebInputException.class.getName())) {
      // avoid logs cluttering for bad requests
      log.trace("Web request for uri {} failed with exception {}", serverRequest.uri(), stackTrace);
      return;
    }
    log.warn("Web request for uri {} failed with exception {}", serverRequest.uri(), stackTrace);
  }
}
