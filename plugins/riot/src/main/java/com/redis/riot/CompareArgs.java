package com.redis.riot;

import java.time.Duration;

import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.reader.DefaultKeyComparator;

import picocli.CommandLine.Option;

public class CompareArgs {

	public static final CompareMode DEFAULT_COMPARE_MODE = CompareMode.QUICK;
	public static final Duration DEFAULT_TTL_TOLERANCE = DefaultKeyComparator.DEFAULT_TTL_TOLERANCE;
	public static final int DEFAULT_TARGET_POOL_SIZE = RedisItemReader.DEFAULT_POOL_SIZE;

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
	private boolean showDiffs;

	@Option(names = "--compare", description = "Comparison mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
	private CompareMode mode = DEFAULT_COMPARE_MODE;

	@Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to consider keys equal (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long ttlTolerance = DEFAULT_TTL_TOLERANCE.toMillis();

	@Option(names = "--target-read-from", description = "Which target cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>", hidden = true)
	private RedisReadFrom targetReadFrom;

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

	public CompareMode getMode() {
		return mode;
	}

	public void setMode(CompareMode mode) {
		this.mode = mode;
	}

	public long getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(long tolerance) {
		this.ttlTolerance = tolerance;
	}

	public RedisReadFrom getTargetReadFrom() {
		return targetReadFrom;
	}

	public void setTargetReadFrom(RedisReadFrom readFrom) {
		this.targetReadFrom = readFrom;
	}

}
