package com.redislabs.riot.redis;

import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.metrics.CommandMetrics;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import picocli.CommandLine;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name="latency")
public class LatencyCommand extends AbstractRedisCommand {

    @CommandLine.Option(names = "--iterations", description = "Number of latency tests (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int iterations = 1000;
    @CommandLine.Option(names = "--sleep", description = "Sleep duration between calls (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
    private long sleep = 1;
    @CommandLine.Option(names = "--unit", description = "Latency unit (default: ${DEFAULT-VALUE})", paramLabel = "<unit>")
    private TimeUnit unit = TimeUnit.MILLISECONDS;
    @CommandLine.Option(names = "--show-distribution", description = "Show latency distribution")
    private boolean showDistribution = false;


    @Override
    protected void execute(BaseRedisCommands<String, String> commands) {
        LatencyStats stats = new LatencyStats();
        for (int index = 0; index < iterations; index++) {
            long startTime = System.nanoTime();
            commands.ping();
            stats.recordLatency(System.nanoTime() - startTime);
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                return;
            }
        }
        Histogram histogram = stats.getIntervalHistogram();
        if (showDistribution) {
            histogram.outputPercentileDistribution(System.out, 1000000.0);
        } else {
            DefaultCommandLatencyCollectorOptions options = DefaultCommandLatencyCollectorOptions.create();
            Map<Double, Long> percentiles = new TreeMap<>();
            for (double targetPercentile : options.targetPercentiles()) {
                percentiles.put(targetPercentile, unit.convert(histogram.getValueAtPercentile(targetPercentile), TimeUnit.NANOSECONDS));
            }
            CommandMetrics.CommandLatency latency = new CommandMetrics.CommandLatency(unit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS), unit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS), percentiles);
            System.out.println(latency.toString());
        }
    }
}
