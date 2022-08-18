package com.rackspace.ceres.app.model;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@AllArgsConstructor
@NoArgsConstructor
public class MetricDTO {

	private String metricName;

	private Map<String,String> tags;

	public MetricDTO(String metricName)	{
		this.metricName = metricName;
	}

	public MetricDTO(Map<String, String> tags)	{
		this.tags = tags;
	}
}
