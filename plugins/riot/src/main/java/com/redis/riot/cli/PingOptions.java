package com.redis.riot.cli;

import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import picocli.CommandLine.Option;

public class PingOptions {

	private static final int DEFAULT_ITERATIONS = 1;
	private static final int DEFAULT_COUNT = 10;
	private static final Duration DEFAULT_SLEEP_DURATION = Duration.ofSeconds(1);
	private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
	private static final double[] DEFAULT_PERCENTILES = DefaultCommandLatencyCollectorOptions.DEFAULT_TARGET_PERCENTILES;

	@Option(names = "--iterations", description = "Number of test iterations. Use a negative value to test endlessly. (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	private int iterations = DEFAULT_ITERATIONS;
	@Option(names = "--count", description = "Number of pings to perform per iteration (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	private int count = DEFAULT_COUNT;
	@Option(names = "--sleep", description = "Sleep duration in seconds between iterations (default: ${DEFAULT-VALUE}).", paramLabel = "<sec>")
	private long sleep = DEFAULT_SLEEP_DURATION.getSeconds();
	@Option(names = "--unit", description = "Time unit used to display latencies (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
	private TimeUnit timeUnit = DEFAULT_TIME_UNIT;
	@Option(names = "--distribution", description = "Show latency distribution.")
	private boolean latencyDistribution;
	@Option(arity = "0..*", names = "--percentiles", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private Set<Double> percentiles = defaultPercentiles();

	private PingOptions(Builder builder) {
		this.iterations = builder.iterations;
		this.count = builder.count;
		this.sleep = builder.sleep.getSeconds();
		this.timeUnit = builder.unit;
		this.latencyDistribution = builder.latencyDistribution;
		this.percentiles = builder.percentiles;
	}

	public static Set<Double> defaultPercentiles() {
		return doubleSet(DEFAULT_PERCENTILES);
	}

	private static Set<Double> doubleSet(double... doubles) {
		return Arrays.stream(doubles).boxed().collect(Collectors.toSet());
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

	public long getSleep() {
		return sleep;
	}

	public void setSleep(long sleep) {
		this.sleep = sleep;
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

	public Set<Double> getPercentiles() {
		return percentiles;
	}

	public void setPercentiles(double... percentiles) {
		setPercentiles(doubleSet(percentiles));
	}

	public void setPercentiles(Set<Double> percentiles) {
		this.percentiles = percentiles;
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private int iterations = DEFAULT_ITERATIONS;
		private int count = DEFAULT_COUNT;
		private Duration sleep = DEFAULT_SLEEP_DURATION;
		private TimeUnit unit = DEFAULT_TIME_UNIT;
		private boolean latencyDistribution;
		private Set<Double> percentiles = defaultPercentiles();

		private Builder() {
		}

		public Builder iterations(int iterations) {
			this.iterations = iterations;
			return this;
		}

		public Builder count(int count) {
			this.count = count;
			return this;
		}

		public Builder sleep(Duration sleep) {
			this.sleep = sleep;
			return this;
		}

		public Builder unit(TimeUnit unit) {
			this.unit = unit;
			return this;
		}

		public Builder percentiles(double... percentiles) {
			this.percentiles = doubleSet(percentiles);
			return this;
		}

		public Builder latencyDistribution(boolean latencyDistribution) {
			this.latencyDistribution = latencyDistribution;
			return this;
		}

		public PingOptions build() {
			return new PingOptions(this);
		}
	}

}
