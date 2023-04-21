package com.redis.riot.cli.operation;

import java.util.Optional;

import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

public class XaddOptions {

	@Mixin
	private FilteringOptions filteringOptions = new FilteringOptions();
	@Option(names = "--maxlen", description = "Stream maxlen.", paramLabel = "<int>")
	private Optional<Long> maxlen = Optional.empty();
	@Option(names = "--trim", description = "Stream efficient trimming ('~' flag).")
	private boolean approximateTrimming;

	public FilteringOptions getFilteringOptions() {
		return filteringOptions;
	}

	public void setFilteringOptions(FilteringOptions filteringOptions) {
		this.filteringOptions = filteringOptions;
	}

	public Optional<Long> getMaxlen() {
		return maxlen;
	}

	public void setMaxlen(long maxlen) {
		this.maxlen = Optional.of(maxlen);
	}

	public boolean isApproximateTrimming() {
		return approximateTrimming;
	}

	public void setApproximateTrimming(boolean approximateTrimming) {
		this.approximateTrimming = approximateTrimming;
	}

	@Override
	public String toString() {
		return "XaddOptions [filteringOptions=" + filteringOptions + ", maxlen=" + maxlen + ", approximateTrimming="
				+ approximateTrimming + "]";
	}

}
