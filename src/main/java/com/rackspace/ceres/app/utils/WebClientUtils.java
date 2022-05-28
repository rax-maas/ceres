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

    public WebClientUtils(DownsampleProperties properties, ObjectMapper objectMapper) {
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    private WebClient webClient() {
        return WebClient.create("http://" + properties.getJobsHost() + ":" + properties.getJobsPort() + "/api/job");
    }

    public Mono<String> claimJob(Job job) {
        return webClient().post()
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(getJobJson(job)))
                .retrieve()
                .bodyToMono(String.class);
    }

    public Mono<String> freeJob(Job job) {
        return webClient().put()
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromValue(getJobJson(job)))
                .retrieve()
                .bodyToMono(String.class);
    }

    public WebClientDTO isLocalJobRequest() {
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getLocalHost();
            Boolean isLocal = inetAddress.getHostName().equals(properties.getJobsHost()) ||
                    inetAddress.getHostAddress().equals(properties.getJobsHost());
            return new WebClientDTO(inetAddress.getHostName(), isLocal);
        } catch (UnknownHostException e) {
            e.printStackTrace();
            return null;
        }
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
