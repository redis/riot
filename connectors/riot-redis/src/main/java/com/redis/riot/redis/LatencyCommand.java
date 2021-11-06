package com.redis.riot.redis;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;

import com.redis.lettucemod.api.sync.RedisModulesCommands;

import io.lettuce.core.metrics.CommandMetrics;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(name = "latency", description = "Calculate latency stats")
public class LatencyCommand extends AbstractRedisCommandCommand {

	@Option(names = "--iterations", description = "Number of latency tests (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
	private int iterations = 1000;
	@Option(names = "--sleep", description = "Sleep duration between calls (default: ${DEFAULT-VALUE})", paramLabel = "<ms>")
	private long sleep = 1;
	@Option(names = "--unit", description = "Latency unit (default: ${DEFAULT-VALUE})", paramLabel = "<unit>")
	private TimeUnit unit = TimeUnit.MILLISECONDS;
	@Option(names = "--show-distribution", description = "Show latency distribution")
	private boolean showDistribution = false;

	@Override
	protected void execute(RedisModulesCommands<String, String> commands) throws InterruptedException {
		LatencyStats stats = new LatencyStats();
		for (int index = 0; index < iterations; index++) {
			long startTime = System.nanoTime();
			commands.ping();
			stats.recordLatency(System.nanoTime() - startTime);
			Thread.sleep(sleep);
		}
		Histogram histogram = stats.getIntervalHistogram();
		if (showDistribution) {
			histogram.outputPercentileDistribution(System.out, 1000000.0);
		} else {
			DefaultCommandLatencyCollectorOptions options = DefaultCommandLatencyCollectorOptions.create();
			Map<Double, Long> percentiles = new TreeMap<>();
			for (double targetPercentile : options.targetPercentiles()) {
				percentiles.put(targetPercentile,
						unit.convert(histogram.getValueAtPercentile(targetPercentile), TimeUnit.NANOSECONDS));
			}
			CommandMetrics.CommandLatency latency = new CommandMetrics.CommandLatency(
					unit.convert(histogram.getMinValue(), TimeUnit.NANOSECONDS),
					unit.convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS), percentiles);
			log.info(latency.toString());
		}
	}

	@Override
	protected String name() {
		return "latency";
	}
}
