package com.rackspace.ceres.app.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rackspace.ceres.app.config.DownsampleProperties;
import com.rackspace.ceres.app.model.Job;
import com.rackspace.ceres.app.model.WebClientDTO;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.InetAddress;
import java.net.UnknownHostException;

@Component
public class WebClientUtils {
    private final DownsampleProperties properties;
    private final ObjectMapper objectMapper;
    private final WebClient webClient;

    public WebClientUtils(DownsampleProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
        String URI = String.format("http://%s:%d/api/job", properties.getJobsHost(), properties.getJobsPort());
        this.webClient = WebClient.create(URI);
    }

    public Mono<String> claimJob(Job job) {
        return this.webClient.post()
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(getJobJson(job)))
                .retrieve()
                .bodyToMono(String.class);
    }

    public Mono<String> freeJob(Job job) {
        return this.webClient.put()
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(getJobJson(job)))
                .retrieve()
                .bodyToMono(String.class);
    }

    public WebClientDTO isLocalJobRequest() {
        WebClientDTO webClientDTO = null;
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            boolean isLocal = inetAddress.getHostName().equals(properties.getJobsHost()) ||
                    inetAddress.getHostAddress().equals(properties.getJobsHost());
            webClientDTO = new WebClientDTO(inetAddress.getHostName(), isLocal);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return webClientDTO;
    }

    private String getJobJson(Job job) {
        String body = null;
        try {
            body = this.objectMapper.writeValueAsString(job);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return body;
    }
}
