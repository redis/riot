package com.redis.riot.gen;

import picocli.CommandLine;

public class GeneratorOptions {

	@CommandLine.Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	protected int start = 0;
	@CommandLine.Option(names = "--end", description = "End index (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
	protected int end = 1000;
	@CommandLine.Option(names = "--sleep", description = "Duration in ms to sleep before each item generation (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long sleep = 0;

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	public long getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
	}

}
