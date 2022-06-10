package com.rackspace.ceres.app.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.Downsampling;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Component
public class WebClientBuffering {
  private final ObjectMapper objectMapper;
  private final WebClient webClient;
  private final DownsampleProperties properties;

  public WebClientBuffering(DownsampleProperties properties, ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
    this.properties = properties;
    String URI = String.format("http://%s:%d/api/downsampling",
        properties.getMongoBufferingHost(), properties.getMongoBufferingPort());
    this.webClient = WebClient.create(URI);
  }

  public Mono<String> saveDownsampling(Integer partition, String group, Instant timeslot, String setHash) {
    Downsampling downsampling = new Downsampling(partition, group, timeslot, setHash);
    return this.webClient.post()
        .accept(MediaType.APPLICATION_JSON)
        .contentType(MediaType.APPLICATION_JSON)
        .body(BodyInserters.fromValue(getDownsamplingJson(downsampling)))
        .retrieve()
        .bodyToMono(String.class);
  }

  private String getDownsamplingJson(Downsampling downsampling) {
    String body = null;
    try {
      body = this.objectMapper.writeValueAsString(downsampling);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return body;
  }
}
