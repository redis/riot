package com.redis.riot;

import java.util.concurrent.TimeUnit;

import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import picocli.CommandLine.Option;

public class LatencyArgs {

	public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;

	public static final double[] DEFAULT_PERCENTILES = DefaultCommandLatencyCollectorOptions.DEFAULT_TARGET_PERCENTILES;

	@Option(names = "--unit", description = "Time unit used to display latencies (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
	private TimeUnit timeUnit = DEFAULT_TIME_UNIT;

	@Option(arity = "0..*", names = "--percent", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<flt>")
	private double[] percentiles = DEFAULT_PERCENTILES;

	public TimeUnit getTimeUnit() {
		return timeUnit;
	}

	public void setTimeUnit(TimeUnit timeUnit) {
		this.timeUnit = timeUnit;
	}

	public double[] getPercentiles() {
		return percentiles;
	}

	public void setPercentiles(double[] percentiles) {
		this.percentiles = percentiles;
	}

}
