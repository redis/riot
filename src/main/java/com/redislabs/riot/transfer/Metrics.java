package com.redislabs.riot.transfer;

import java.util.List;

import lombok.Builder;
import lombok.Data;
import lombok.experimental.Accessors;

@Builder
@Accessors(fluent = true)
public @Data class Metrics {

	private long reads;
	private long writes;
	private int runningThreads;

	public static Metrics create(List<Metrics> metrics) {
		long reads = 0;
		long writes = 0;
		int runningThreads = 0;
		for (Metrics metric : metrics) {
			reads += metric.reads();
			writes += metric.writes();
			runningThreads += metric.runningThreads();
		}
		return Metrics.builder().reads(reads).writes(writes).runningThreads(runningThreads).build();
	}
}