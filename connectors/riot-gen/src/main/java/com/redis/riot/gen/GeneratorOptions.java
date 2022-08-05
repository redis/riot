package com.redis.riot.gen;

import java.util.Optional;

import picocli.CommandLine.Option;

public class GeneratorOptions {

	@Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	protected int start = 1;
	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	protected int count = 1000;
	@Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private Optional<Long> sleep = Optional.empty();

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

	public Optional<Long> getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = Optional.of(sleep);
	}

	@Override
	public String toString() {
		return "GeneratorOptions [start=" + start + ", count=" + count + ", sleep=" + sleep + "]";
	}

}
