package com.redis.riot.cli;

import java.util.concurrent.TimeUnit;

import com.redis.riot.core.Executable;
import com.redis.riot.core.PingExecutable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ping", description = "Test connectivity to a Redis server.")
public class Ping extends AbstractLoggingCommand<Main> {

    @Option(names = "--iterations", description = "Number of test iterations. Use a negative value to test endlessly. (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
    private int iterations = com.redis.riot.core.PingExecutable.DEFAULT_ITERATIONS;

    @Option(names = "--count", description = "Number of pings to perform per iteration (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
    private int count = com.redis.riot.core.PingExecutable.DEFAULT_COUNT;

    @Option(names = "--unit", description = "Time unit used to display latencies (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
    private TimeUnit timeUnit = com.redis.riot.core.PingExecutable.DEFAULT_TIME_UNIT;

    @Option(names = "--distribution", description = "Show latency distribution.")
    private boolean latencyDistribution;

    @Option(arity = "0..*", names = "--percentiles", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
    private double[] percentiles = com.redis.riot.core.PingExecutable.DEFAULT_PERCENTILES;

    @Override
    protected Executable getExecutable() {
        PingExecutable ping = new PingExecutable(parent.getRedisArgs().client(), System.out);
        ping.setCount(count);
        ping.setIterations(iterations);
        ping.setLatencyDistribution(latencyDistribution);
        ping.setTimeUnit(timeUnit);
        ping.setPercentiles(percentiles);
        return ping;
    }

}
