package com.redislabs.recharge.config;

public class FlatFile {

	private Integer linesToSkip;
	private String[] fieldNames;
	private Delimited delimited = new Delimited();
	private FixedLength fixedLength = new FixedLength();

	public FixedLength getFixedLength() {
		return fixedLength;
	}

	public void setFixedLength(FixedLength fixedLength) {
		this.fixedLength = fixedLength;
	}

	public Delimited getDelimited() {
		return delimited;
	}

	public void setDelimited(Delimited delimited) {
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