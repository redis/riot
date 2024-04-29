package com.redis.riot.cli;

import java.util.concurrent.TimeUnit;

import com.redis.riot.redis.Ping;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "ping", description = "Test connectivity to a Redis server.")
public class PingCommand extends AbstractRiotCommand {

	@ParentCommand
	private AbstractMainCommand parent;

	@ArgGroup(exclusive = false, heading = "Redis client options%n")
	private RedisClientArgs redisClientArgs = new RedisClientArgs();

	@Option(names = "--iterations", description = "Number of test iterations. Use a negative value to test endlessly. (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	private int iterations = Ping.DEFAULT_ITERATIONS;

	@Option(names = "--count", description = "Number of pings to perform per iteration (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	private int count = Ping.DEFAULT_COUNT;

	@Option(names = "--unit", description = "Time unit used to display latencies (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
	private TimeUnit timeUnit = Ping.DEFAULT_TIME_UNIT;

	@Option(names = "--distribution", description = "Show latency distribution.")
	private boolean latencyDistribution;

	@Option(arity = "0..*", names = "--percentiles", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private double[] percentiles = Ping.defaultPercentiles();

	@Override
	protected Ping callable() {
		Ping ping = new Ping();
		ping.setRedisClientOptions(redisClientArgs.redisClientOptions());
		ping.setOut(parent.out);
		ping.setCount(count);
		ping.setIterations(iterations);
		ping.setLatencyDistribution(latencyDistribution);
		ping.setTimeUnit(timeUnit);
		ping.setPercentiles(percentiles);
		return ping;
	}

	public RedisClientArgs getRedisClientArgs() {
		return redisClientArgs;
	}

	public void setRedisClientArgs(RedisClientArgs args) {
		this.redisClientArgs = args;
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

	public void setTimeUnit(TimeUnit timeUnit) {
		this.timeUnit = timeUnit;
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

}
