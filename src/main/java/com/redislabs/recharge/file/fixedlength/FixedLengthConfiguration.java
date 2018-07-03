package com.redislabs.recharge.file.fixedlength;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
public class FixedLengthConfiguration {

	private String[] ranges;
	private Boolean strict;

	public String[] getRanges() {
		return ranges;
	}

	public void setRanges(String[] ranges) {
		this.ranges = ranges;
	}

	public Boolean getStrict() {
		return strict;
	}

	public void setStrict(Boolean strict) {
		this.strict = strict;
	}
}
