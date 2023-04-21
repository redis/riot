package com.redis.riot.cli;

import java.time.Duration;

import picocli.CommandLine.Option;

public class CompareOptions {

	public static final long DEFAULT_TTL_TOLERANCE_IN_SECONDS = 1;

	@Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long ttlTolerance = DEFAULT_TTL_TOLERANCE_IN_SECONDS;
	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification.")
	private boolean showDiffs;

	public long getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(long ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public Duration getTtlToleranceDuration() {
		return Duration.ofSeconds(ttlTolerance);
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

}
