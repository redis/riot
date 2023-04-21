package com.redis.riot.cli;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.step.tasklet.CallableTaskletAdapter;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;
import org.threeten.bp.Duration;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;

import io.lettuce.core.metrics.CommandMetrics;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;

@Command(name = "ping", description = "Test connection to a Redis database")
public class PingCommand extends AbstractJobCommand {

	private static final String COMMAND_NAME = "ping";
	private static final PrintStream PRINT_STREAM = System.out;

	@Mixin
	private PingOptions options = PingOptions.builder().build();

	@Override
	protected Job job(JobCommandContext context) {
		CallableTaskletAdapter tasklet = new CallableTaskletAdapter();
		tasklet.setCallable(new PingTask(context, options));
		TaskletStep step = context.step(COMMAND_NAME).tasklet(tasklet).build();
		return context.job(COMMAND_NAME).start(step).build();
	}

	private class PingTask implements Callable<RepeatStatus> {

		private final JobCommandContext context;
		private final PingOptions options;
		private final AtomicInteger iteration = new AtomicInteger();

		public PingTask(JobCommandContext context, PingOptions options) {
			this.context = context;
			this.options = options;
		}

		@Override
		public RepeatStatus call() throws Exception {
			if (iteration.get() > 0) {
				Thread.sleep(Duration.ofSeconds(options.getSleep()).toMillis());
			}
			try (StatefulRedisModulesConnection<String, String> connection = context.connection()) {
				execute(connection);
			}
			if (iteration.incrementAndGet() < options.getIterations()) {
				return RepeatStatus.CONTINUABLE;
			}
			return RepeatStatus.FINISHED;
		}

		private void execute(StatefulRedisModulesConnection<String, String> connection) {
			LatencyStats stats = new LatencyStats();
			for (int index = 0; index < options.getCount(); index++) {
				long startTime = System.nanoTime();
				String reply = connection.sync().ping();
				Assert.isTrue("pong".equalsIgnoreCase(reply), "Invalid PING reply received: " + reply);
				stats.recordLatency(System.nanoTime() - startTime);
			}
			Histogram histogram = stats.getIntervalHistogram();
			if (options.isLatencyDistribution()) {
				histogram.outputPercentileDistribution(PRINT_STREAM, (double) options.getTimeUnit().toNanos(1));
			}
			Map<Double, Long> percentiles = new TreeMap<>();
			for (double targetPercentile : options.getPercentiles()) {
				percentiles.put(targetPercentile,
						convert(histogram.getValueAtPercentile(targetPercentile), TimeUnit.NANOSECONDS));
			}
			CommandMetrics.CommandLatency latency = new CommandMetrics.CommandLatency(
					convert(histogram.getMinValue(), TimeUnit.NANOSECONDS),
					convert(histogram.getMaxValue(), TimeUnit.NANOSECONDS), percentiles);
			PRINT_STREAM.println(latency.toString());
		}

		private long convert(long value, TimeUnit unit) {
			return options.getTimeUnit().convert(value, unit);
		}
	}

}
