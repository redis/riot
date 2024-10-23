package com.redis.riot;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.HdrHistogram.Histogram;
import org.LatencyUtils.LatencyStats;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.core.IO;
import com.redis.riot.core.Step;

import io.lettuce.core.metrics.CommandMetrics.CommandLatency;
import io.lettuce.core.metrics.DefaultCommandLatencyCollectorOptions;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "ping", description = "Test connectivity to a Redis server.")
public class Ping extends AbstractRedisCommand {

	private static final String TASK_NAME = "Pinging";

	public static final int DEFAULT_COUNT = 1000;
	public static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.MILLISECONDS;
	private static final List<Double> DEFAULT_PERCENTILES = DoubleStream
			.of(DefaultCommandLatencyCollectorOptions.DEFAULT_TARGET_PERCENTILES).boxed().collect(Collectors.toList());

	@ParentCommand
	IO parent;

	@Option(names = "--unit", description = "Time unit used to display latencies: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<unit>")
	private TimeUnit timeUnit = DEFAULT_TIME_UNIT;

	@Option(arity = "0..*", names = "--pcent", description = "Latency percentiles to display (default: ${DEFAULT-VALUE}).", paramLabel = "<p>")
	private Set<Double> percentiles = defaultPercentiles();

	@Option(names = "--count", description = "Number of pings to execute (default: ${DEFAULT-VALUE}).", paramLabel = "<count>")
	private int count = DEFAULT_COUNT;

	@Override
	protected Job job() {
		PingExecutionItemReader reader = new PingExecutionItemReader(commands());
		reader.setMaxItemCount(count);
		PingLatencyItemWriter writer = new PingLatencyItemWriter();
		Step<PingExecution, PingExecution> step = new Step<>(reader, writer);
		step.taskName(TASK_NAME);
		step.maxItemCount(count);
		return job(step);
	}

	public static Set<Double> defaultPercentiles() {
		return new LinkedHashSet<>(DEFAULT_PERCENTILES);
	}

	class PingLatencyItemWriter implements ItemWriter<PingExecution> {

		@Override
		public void write(Chunk<? extends PingExecution> chunk) throws Exception {
			LatencyStats stats = new LatencyStats();
			for (PingExecution execution : chunk) {
				if (execution.isSuccess()) {
					stats.recordLatency(execution.getDuration().toNanos());
				} else {
					log.error("Invalid PING reply received: {}", execution.getReply());
				}
			}
			parent.getOut().println(commandLatency(stats));
		}

		private CommandLatency commandLatency(LatencyStats stats) {
			Histogram histogram = stats.getIntervalHistogram();
			Map<Double, Long> map = percentiles.stream()
					.collect(Collectors.toMap(Function.identity(), p -> time(histogram.getValueAtPercentile(p))));
			return new CommandLatency(time(histogram.getMinValue()), time(histogram.getMaxValue()), map);
		}

		private long time(long value) {
			return timeUnit.convert(value, TimeUnit.NANOSECONDS);
		}

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

	public Set<Double> getPercentiles() {
		return percentiles;
	}

	public void setPercentiles(Set<Double> percentiles) {
		this.percentiles = percentiles;
	}

}
