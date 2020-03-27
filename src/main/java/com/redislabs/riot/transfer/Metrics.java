package com.redislabs.riot.transfer;

import java.util.List;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

public class Metrics {

	private @Getter @Setter long reads;
	private @Getter @Setter long writes;
	private @Getter @Setter int runningThreads;

	@Builder
	protected Metrics(long reads, long writes, int runningThreads, List<Metrics> metrics) {
		this.reads = reads;
		this.writes = writes;
		this.runningThreads = runningThreads;
		if (metrics != null) {
			for (Metrics metric : metrics) {
				this.reads += metric.getReads();
				this.writes += metric.getWrites();
				this.runningThreads += metric.getRunningThreads();
			}
		}
	}
}