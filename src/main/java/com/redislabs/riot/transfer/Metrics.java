package com.redislabs.riot.transfer;

import java.util.List;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class Metrics {

	private @Getter @Setter long reads;
	private @Getter @Setter long writes;
	private @Getter @Setter int runningThreads;

	public static Metrics create(List<Metrics> metrics) {
		long reads = 0;
		long writes = 0;
		int runningThreads = 0;
		for (Metrics metric : metrics) {
			reads += metric.reads();
			writes += metric.writes();
			runningThreads += metric.runningThreads();
		}
		return new Metrics().reads(reads).writes(writes).runningThreads(runningThreads);
	}
}