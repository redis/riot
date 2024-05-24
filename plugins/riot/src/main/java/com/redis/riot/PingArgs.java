package com.redis.riot;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class PingArgs {

	public static final int DEFAULT_COUNT = 1000;

	@Option(names = "--count", description = "Number of pings to execute (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	private int count = DEFAULT_COUNT;

	@ArgGroup(exclusive = false)
	private LatencyArgs latencyArgs = new LatencyArgs();

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public LatencyArgs getLatencyArgs() {
		return latencyArgs;
	}

	public void setLatencyArgs(LatencyArgs latencyArgs) {
		this.latencyArgs = latencyArgs;
	}

}
