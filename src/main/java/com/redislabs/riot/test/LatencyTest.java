package com.redislabs.riot.test;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.metrics.CommandMetrics.CommandLatency;
import redis.clients.jedis.Jedis;

public class LatencyTest implements RedisTest {

	private int iterations;
	private long sleep;
	private TimeUnit timeUnit;
	private boolean printDistribution;

	public LatencyTest(int iterations, long sleep, TimeUnit timeUnit, boolean printDistribution) {
		this.iterations = iterations;
		this.sleep = sleep;
		this.timeUnit = timeUnit;
		this.printDistribution = printDistribution;
	}

	@Override
	public void execute(Jedis jedis) throws InterruptedException {
		execute(jedis::ping);
	}

	@Override
	public void execute(BaseRedisCommands<String, String> commands) throws InterruptedException {
		execute(commands::ping);
	}

	private <T> void execute(Supplier<T> call) throws InterruptedException {
		LatencyStats stats = new LatencyStats();
		for (int index = 0; index < iterations; index++) {
			long startTime = System.nanoTime();
			call.get();
			stats.recordLatency(System.nanoTime() - startTime);
			Thread.sleep(sleep);
		}
		Histogram histogram = stats.getIntervalHistogram();
		if (printDistribution) {
			histogram.outputPercentileDistribution(System.out, 1000000.0);
		} else {
			DefaultCommandLatencyCollectorOptions options = DefaultCommandLatencyCollectorOptions.create();
			Map<Double, Long> percentiles = new TreeMap<Double, Long>();
			for (double targetPercentile : options.targetPercentiles()) {
				percentiles.put(targetPercentile,
						timeUnit.convert(histogram.getValueAtPercentile(targetPercentile), TimeUnit.NANOSECONDS));
			}
			CommandLatency latency = new CommandLatency(timeUnit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS),
					timeUnit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS), percentiles);
			System.out.println(latency.toString());
		}
	}

}
