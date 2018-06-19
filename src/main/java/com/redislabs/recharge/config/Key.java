package com.redislabs.recharge.config;

public class Key {

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

	public void setFields(String[] keyFields) {
		this.fields = keyFields;
	}
}