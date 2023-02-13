package com.redis.riot.gen;

import com.redis.riot.ProgressMonitor.Builder;

import picocli.CommandLine.Option;

public class GeneratorOptions {

	@Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int start = 1;
	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int count = 1000;

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public Builder configure(Builder monitor) {
		return monitor.initialMax(() -> (long) count);
	}

	@Override
	public String toString() {
		return "GeneratorOptions [start=" + start + ", count=" + count + "]";
	}

}
