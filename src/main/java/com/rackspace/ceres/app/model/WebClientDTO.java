package com.rackspace.ceres.app.model;

import lombok.Data;

@Data
public class WebClientDTO {
    private String hostName;
    private Boolean isLocalJobRequest;

    public WebClientDTO(String hostName, Boolean isLocalJobRequest) {
        this.hostName = hostName;
        this.isLocalJobRequest = isLocalJobRequest;
    }
}
