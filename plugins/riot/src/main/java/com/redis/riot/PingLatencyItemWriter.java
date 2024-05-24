package com.redis.riot;

import java.io.PrintWriter;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import io.lettuce.core.metrics.CommandMetrics.CommandLatency;

public class PingLatencyItemWriter implements ItemWriter<PingExecution> {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final PrintWriter out;

	private LatencyArgs latencyArgs = new LatencyArgs();

	public PingLatencyItemWriter(PrintWriter out) {
		this.out = out;
	}

	@Override
	public void write(Chunk<? extends PingExecution> chunk) throws Exception {
		LatencyStats stats = new LatencyStats();
		for (PingExecution execution : chunk) {
			if (execution.isSuccess()) {
				stats.recordLatency(execution.getDuration().toNanos());
			} else {
				log.error("Invalid PING reply received: {}", execution.getReply());
			}
		}
		out.println(commandLatency(stats));
	}

	private CommandLatency commandLatency(LatencyStats stats) {
		Histogram histogram = stats.getIntervalHistogram();
		Map<Double, Long> percentiles = new TreeMap<>();
		for (double targetPercentile : latencyArgs.getPercentiles()) {
			percentiles.put(targetPercentile, time(histogram.getValueAtPercentile(targetPercentile)));
		}
		return new CommandLatency(time(histogram.getMinValue()), time(histogram.getMaxValue()), percentiles);
	}

	private long time(long value) {
		return latencyArgs.getTimeUnit().convert(value, TimeUnit.NANOSECONDS);
	}

	public LatencyArgs getLatencyArgs() {
		return latencyArgs;
	}

	public void setLatencyArgs(LatencyArgs latencyArgs) {
		this.latencyArgs = latencyArgs;
	}
}