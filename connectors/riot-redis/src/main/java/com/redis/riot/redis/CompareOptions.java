package com.redis.riot.redis;

import java.time.Duration;

import picocli.CommandLine;

public class CompareOptions {

	public static final long DEFAULT_TTL_TOLERANCE_IN_SECONDS = 1;

	@CommandLine.Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long ttlTolerance = DEFAULT_TTL_TOLERANCE_IN_SECONDS;
	@CommandLine.Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification")
	private boolean showDiffs;

	public Duration getTtlToleranceDuration() {
		return Duration.ofSeconds(ttlTolerance);
	}

	public long getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(long ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}
}
