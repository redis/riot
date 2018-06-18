package com.redislabs.recharge.config;

public class Redis {

	private String keyPrefix;

	private String keySeparator = ":";

	private String[] keyFields;

	public String getKeyPrefix() {
		return keyPrefix;
	}

	public void setKeyPrefix(String keyPrefix) {
		this.keyPrefix = keyPrefix;
	}

	public String getKeySeparator() {
		return keySeparator;
	}

	public void setKeySeparator(String keySeparator) {
		this.keySeparator = keySeparator;
	}

	public String[] getKeyFields() {
		return keyFields;
	}

	public void setKeyFields(String[] keyFields) {
		this.keyFields = keyFields;
	}
}