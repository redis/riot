package com.redislabs.recharge.generator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@Data
public class GeneratorConfiguration {

	private String map;
	private Map<String, String> fields = new LinkedHashMap<>();
	private String locale = "en-US";

	public GeneratorReader reader() {
		GeneratorReader reader = new GeneratorReader();
		reader.setFields(fields);
		reader.setLocale(locale);
		reader.setMapExpression(map);
		return reader;
	}
}