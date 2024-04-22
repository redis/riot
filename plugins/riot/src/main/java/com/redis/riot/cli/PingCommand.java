package com.redis.riot.cli;

import java.util.concurrent.TimeUnit;

import com.redis.riot.redis.Ping;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ping", description = "Test connectivity to a Redis server.")
public class PingCommand extends AbstractRiotCommand {

	@Option(names = "--iterations", description = "Number of test iterations. Use a negative value to test endlessly. (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	int iterations = Ping.DEFAULT_ITERATIONS;

	@Option(names = "--count", description = "Number of pings to perform per iteration (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	int count = Ping.DEFAULT_COUNT;

	@Option(names = "--unit", description = "Time unit used to display latencies (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
	TimeUnit timeUnit = Ping.DEFAULT_TIME_UNIT;

	@Option(names = "--distribution", description = "Show latency distribution.")
	boolean latencyDistribution;

	@Option(arity = "0..*", names = "--percentiles", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	double[] percentiles = Ping.defaultPercentiles();

	@Override
	protected Ping runnable() {
		Ping runnable = new Ping();
		runnable.setOut(parent.out);
		runnable.setCount(count);
		runnable.setIterations(iterations);
		runnable.setLatencyDistribution(latencyDistribution);
		runnable.setTimeUnit(timeUnit);
		runnable.setPercentiles(percentiles);
		return runnable;
	}

}
