package com.redislabs.recharge.config;

import java.util.LinkedHashMap;
import java.util.Map;

import lombok.Data;

@Data
public class GeneratorConfiguration {

	Map<String, String> fields = new LinkedHashMap<>();
	String locale;

	public boolean isEnabled() {
		return !fields.isEmpty();
	}

}
