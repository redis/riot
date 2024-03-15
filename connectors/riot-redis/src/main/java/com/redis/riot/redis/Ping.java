package com.redis.riot.redis;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.springframework.util.Assert;

import com.redis.riot.core.AbstractRunnable;

import io.lettuce.core.metrics.CommandMetrics.CommandLatency;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;

public class Ping extends AbstractRunnable {

	public static final int DEFAULT_ITERATIONS = 1;
	public static final int DEFAULT_COUNT = 10;
	public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
	private static final double[] DEFAULT_PERCENTILES = DefaultCommandLatencyCollectorOptions.DEFAULT_TARGET_PERCENTILES;

	private PrintWriter out;
	private int iterations = DEFAULT_ITERATIONS;
	private int count = DEFAULT_COUNT;
	private TimeUnit timeUnit = DEFAULT_TIME_UNIT;
	private boolean latencyDistribution;
	private double[] percentiles = DEFAULT_PERCENTILES;
	private Duration sleep;

	public void setOut(PrintWriter out) {
		this.out = out;
	}

	public void setSleep(Duration sleep) {
		this.sleep = sleep;
	}

	public Duration getSleep() {
		return sleep;
	}

	public int getIterations() {
		return iterations;
	}

	public void setIterations(int iterations) {
		this.iterations = iterations;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

	public void setTimeUnit(TimeUnit unit) {
		this.timeUnit = unit;
	}

	public boolean isLatencyDistribution() {
		return latencyDistribution;
	}

	public void setLatencyDistribution(boolean latencyDistribution) {
		this.latencyDistribution = latencyDistribution;
	}

	public double[] getPercentiles() {
		return percentiles;
	}

	public void setPercentiles(double[] percentiles) {
		this.percentiles = percentiles;
	}

	@Override
	protected void doRun() {
		for (int iteration = 0; iteration < iterations; iteration++) {
			LatencyStats stats = new LatencyStats();
			for (int index = 0; index < count; index++) {
				long startTime = System.nanoTime();
				String reply = getRedisConnection().sync().ping();
				Assert.isTrue("pong".equalsIgnoreCase(reply), "Invalid PING reply received: " + reply);
				stats.recordLatency(System.nanoTime() - startTime);
			}
			Histogram histogram = stats.getIntervalHistogram();
			if (latencyDistribution) {
				histogram.outputPercentileDistribution(System.out, (double) timeUnit.toNanos(1));
			}
			Map<Double, Long> percentileMap = new TreeMap<>();
			for (double targetPercentile : percentiles) {
				long percentile = toTimeUnit(histogram.getValueAtPercentile(targetPercentile));
				percentileMap.put(targetPercentile, percentile);
			}
			long min = toTimeUnit(histogram.getMinValue());
			long max = toTimeUnit(histogram.getMaxValue());
			CommandLatency latency = new CommandLatency(min, max, percentileMap);
			out.println(latency.toString());
			if (sleep != null) {
				try {
					Thread.sleep(sleep.toMillis());
				} catch (InterruptedException e) {
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private long toTimeUnit(long value) {
		return timeUnit.convert(value, TimeUnit.NANOSECONDS);
	}

	public static double[] defaultPercentiles() {
		return DEFAULT_PERCENTILES;
	}

}
