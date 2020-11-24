package com.rackspace.ceres.app.web;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.web.ResourceProperties;
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
import org.springframework.web.reactive.function.server.RequestPredicates;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@Order(-2)//So that our exception handler gets picked before DefaultErrorWebExceptionHandler
public class GlobalErrorWebExceptionHandler extends
    AbstractErrorWebExceptionHandler {

  private final String ILLEGAL_ARGUMENT_EXCEPTION = "java.lang.IllegalArgumentException";

  public GlobalErrorWebExceptionHandler(
      ErrorAttributes errorAttributes,
      ResourceProperties resourceProperties,
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

  private Mono<ServerResponse> renderErrorResponse(ServerRequest serverRequest) {
    Map<String, Object> body = getErrorAttributes(serverRequest, ErrorAttributeOptions.of(
        Include.EXCEPTION, Include.MESSAGE, Include.STACK_TRACE));
    String exceptionClass = (String) body.get("exception");
    logErrorMessage(serverRequest, (String) body.get("trace"));
    return respondWith(body, exceptionClass);
  }

  private Mono<ServerResponse> respondWith(Map<String, Object> body, String exceptionClass) {
    if (exceptionClass.equals(ILLEGAL_ARGUMENT_EXCEPTION)) {
      body.remove("error");
      body.remove("trace");
      body.put("status", HttpStatus.BAD_REQUEST.value());
      return ServerResponse.status(HttpStatus.BAD_REQUEST).body(BodyInserters.fromValue(body));
    }
    return ServerResponse.status(HttpStatus.INTERNAL_SERVER_ERROR).body(BodyInserters.fromValue(
        body));
  }

  private void logErrorMessage(ServerRequest serverRequest, String stackTrace) {
    log.warn("Web request for uri {} failed with exception {}", serverRequest.uri(), stackTrace);
  }
}