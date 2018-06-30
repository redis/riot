package com.redislabs.recharge.config;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "key")
@EnableAutoConfiguration
public class KeyConfiguration {

	private String prefix;

	private String separator = ":";

	private String[] fields;

	public String getPrefix() {
		return prefix;
	}

	public void setPrefix(String keyPrefix) {
		this.prefix = keyPrefix;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String keySeparator) {
		this.separator = keySeparator;
	}

	public String[] getFields() {
		return fields;
	}

	public void setFields(String... keyFields) {
		this.fields = keyFields;
	}
}