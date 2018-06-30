package com.redislabs.recharge.file;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "file.flat")
@EnableAutoConfiguration
public class FlatFileConfiguration {

	private Integer linesToSkip;
	private String[] fieldNames;
	private DelimitedConfiguration delimited = new DelimitedConfiguration();
	private FixedLengthConfiguration fixedLength = new FixedLengthConfiguration();

	public FixedLengthConfiguration getFixedLength() {
		return fixedLength;
	}

	public void setFixedLength(FixedLengthConfiguration fixedLength) {
		this.fixedLength = fixedLength;
	}

	public DelimitedConfiguration getDelimited() {
		return delimited;
	}

	public void setDelimited(DelimitedConfiguration delimited) {
		this.delimited = delimited;
	}

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