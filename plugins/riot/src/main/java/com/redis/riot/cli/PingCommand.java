package com.redis.riot.cli;

import java.util.concurrent.TimeUnit;

import com.redis.riot.core.Executable;
import com.redis.riot.core.Ping;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ping", description = "Test connectivity to a Redis server.")
public class PingCommand extends AbstractCommand {

    @Option(names = "--iterations", description = "Number of test iterations. Use a negative value to test endlessly. (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
    private int iterations = com.redis.riot.core.Ping.DEFAULT_ITERATIONS;

    @Option(names = "--count", description = "Number of pings to perform per iteration (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
    private int count = com.redis.riot.core.Ping.DEFAULT_COUNT;

    @Option(names = "--unit", description = "Time unit used to display latencies (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
    private TimeUnit timeUnit = com.redis.riot.core.Ping.DEFAULT_TIME_UNIT;

    @Option(names = "--distribution", description = "Show latency distribution.")
    private boolean latencyDistribution;

    @Option(arity = "0..*", names = "--percentiles", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
    private double[] percentiles = com.redis.riot.core.Ping.DEFAULT_PERCENTILES;

    @Override
    protected Executable getExecutable() {
        Ping executable = new Ping(parent.getRedisArgs().client(), parent.getOut());
        executable.setCount(count);
        executable.setIterations(iterations);
        executable.setLatencyDistribution(latencyDistribution);
        executable.setTimeUnit(timeUnit);
        executable.setPercentiles(percentiles);
        return executable;
    }

}
