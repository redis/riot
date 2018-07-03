package com.redislabs.recharge.file;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
public class FlatFileConfiguration {

	private Integer linesToSkip;
	private String[] fieldNames;

	public Integer getLinesToSkip() {
		return linesToSkip;
	}

	public void setLinesToSkip(int linesToSkip) {
		this.linesToSkip = linesToSkip;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(String[] fieldNames) {
		this.fieldNames = fieldNames;
	}

}