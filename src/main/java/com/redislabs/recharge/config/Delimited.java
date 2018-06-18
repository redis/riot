package com.redislabs.recharge.config;

public class Delimited {

	private String delimiter;
	private Integer[] includedFields;
	private Character quoteCharacter;

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public Integer[] getIncludedFields() {
		return includedFields;
	}

	public void setIncludedFields(Integer[] includedFields) {
		this.includedFields = includedFields;
	}

	public Character getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(Character quoteCharacter) {
		this.quoteCharacter = quoteCharacter;
	}
}