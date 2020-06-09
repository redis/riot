package com.redislabs.riot.cli;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.metrics.CommandMetrics;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import io.lettuce.core.protocol.RedisCommand;
import lombok.extern.slf4j.Slf4j;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Command(name = "test", description = "Execute a test")
public class TestCommand extends RiotCommand implements Runnable {

    public enum RedisTestType {
        INFO, PING, LATENCY
    }

    @Option(names = {"-t", "--test"}, description = "Test to execute: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
    private RedisTestType test = RedisTestType.PING;
    @Option(names = "--latency-iterations", description = "Number of iterations for latency test (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int latencyIterations = 1000;
    @Option(names = "--latency-sleep", description = "Sleep duration in milliseconds between calls (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long latencySleep = 1;
    @Option(names = "--latency-unit", description = "Latency unit (default: ${DEFAULT-VALUE})", paramLabel = "<unit>")
    private TimeUnit latencyTimeUnit = TimeUnit.MILLISECONDS;
    @Option(names = "--latency-distribution", description = "Show latency distribution")
    private boolean latencyDistribution = false;

    @Override
    public void run() {
        BaseRedisCommands<String, String> commands = getRedisCommands();
        switch (test) {
            case PING:
                ping(commands);
            case INFO:
                info(commands);
            case LATENCY:
                latency(commands);
        }
    }

    private BaseRedisCommands<String, String> getRedisCommands() {
        if (getRedisOptions().isCluster()) {
            return getRedisOptions().redisClusterClient().connect().sync();
        }
        return getRedisOptions().redisClient().connect().sync();
    }

    private void ping(BaseRedisCommands<String, String> commands) {
        log.info("Received ping reply: {}", commands.ping());
    }

    private void info(BaseRedisCommands<String, String> commands) {
        log.info(((RedisServerCommands<String, String>) commands).info());
    }

    private void latency(BaseRedisCommands<String, String> commands) {
        LatencyStats stats = new LatencyStats();
        for (int index = 0; index < latencyIterations; index++) {
            long startTime = System.nanoTime();
            commands.ping();
            stats.recordLatency(System.nanoTime() - startTime);
            try {
                Thread.sleep(latencySleep);
            } catch (InterruptedException e) {
                return;
            }
        }
        Histogram histogram = stats.getIntervalHistogram();
        if (latencyDistribution) {
            histogram.outputPercentileDistribution(System.out, 1000000.0);
        } else {
            DefaultCommandLatencyCollectorOptions options = DefaultCommandLatencyCollectorOptions.create();
            Map<Double, Long> percentiles = new TreeMap<>();
            for (double targetPercentile : options.targetPercentiles()) {
                percentiles.put(targetPercentile, latencyTimeUnit.convert(histogram.getValueAtPercentile(targetPercentile), TimeUnit.NANOSECONDS));
            }
            CommandMetrics.CommandLatency latency = new CommandMetrics.CommandLatency(latencyTimeUnit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS), latencyTimeUnit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS), percentiles);
            System.out.println(latency.toString());
        }
    }

}
