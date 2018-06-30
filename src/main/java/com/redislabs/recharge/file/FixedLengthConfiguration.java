package com.redislabs.recharge.file;

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
