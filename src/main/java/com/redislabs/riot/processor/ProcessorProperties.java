package com.redislabs.riot.processor;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Data;

@Data
@ConfigurationProperties(prefix = "processor")
public class ProcessorProperties {

	private String source;
	private String merge;
	private Map<String, String> fields = new LinkedHashMap<>();

}
