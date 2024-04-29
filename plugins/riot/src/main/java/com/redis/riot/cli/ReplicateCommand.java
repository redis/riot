package com.redis.riot.cli;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.redis.CompareMode;
import com.redis.riot.redis.KeyComparisonStatusCountItemWriter;
import com.redis.riot.redis.Replication;
import com.redis.riot.redis.ReplicationMode;
import com.redis.riot.redis.ReplicationType;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyComparatorOptions;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyNotificationItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class ReplicateCommand extends AbstractRiotCommand {

	private static final Status[] STATUSES = { Status.OK, Status.MISSING, Status.TYPE, Status.VALUE, Status.TTL };
	private static final String QUEUE_MESSAGE = " | queue capacity: %,d";
	private static final String NUMBER_FORMAT = "%,d";
	private static final String COMPARE_MESSAGE = compareMessageFormat();
	private static final Map<String, String> taskNames = taskNames();

	@Option(names = "--mode", description = { "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).",
			"SNAPSHOT: initial replication using key scan.",
			"LIVE: initial and continuous replication using key scan and keyspace notifications in parallel.",
			"LIVEONLY: continuous replication using keyspace notifications (only changed keys are replicated).",
			"COMPARE: compare source and target database." }, paramLabel = "<name>")
	private ReplicationMode mode = ReplicationMode.SNAPSHOT;

	@Option(names = "--type", description = "Enable type-based replication")
	private boolean type;

	@Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long ttlTolerance = KeyComparatorOptions.DEFAULT_TTL_TOLERANCE.toMillis();

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
	private boolean showDiffs;

	@Option(names = "--compare", description = "Comparison mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
	private CompareMode compareMode = Replication.DEFAULT_COMPARE_MODE;

	@Option(names = "--flush-interval", description = "Max duration in millis between flushes (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	private long flushInterval = Replication.DEFAULT_FLUSH_INTERVAL.toMillis();

	@Option(names = "--idle-timeout", description = "Min number of millis to consider transfer complete (default: no timeout).", paramLabel = "<ms>")
	private long idleTimeout = Replication.DEFAULT_IDLE_TIMEOUT.toMillis();

	@ArgGroup(exclusive = false, heading = "Source Redis client options%n")
	private RedisClientArgs sourceRedisClientArgs = new RedisClientArgs();

	@ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
	private ReplicateRedisReaderArgs sourceRedisReaderArgs = new ReplicateRedisReaderArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@ArgGroup(exclusive = false, heading = "Target Redis client options%n")
	private ReplicateTargetRedisClientArgs targetRedisClientArgs = new ReplicateTargetRedisClientArgs();

	@ArgGroup(exclusive = false, heading = "Target Redis writer options%n")
	private RedisWriterArgs targetRedisWriterArgs = new RedisWriterArgs();

	private static Map<String, String> taskNames() {
		Map<String, String> map = new HashMap<>();
		map.put(Replication.STEP_SCAN, "Scanning");
		map.put(Replication.STEP_LIVE, "Listening");
		map.put(Replication.STEP_COMPARE, "Comparing");
		return map;
	}

	@Override
	protected Replication callable() {
		Replication replication = new Replication();
		replication.setCompareMode(compareMode);
		replication.setMode(mode);
		replication.setShowDiffs(showDiffs);
		if (targetRedisClientArgs.getReadFrom() != null) {
			replication.setTargetReadFrom(targetRedisClientArgs.getReadFrom().getReadFrom());
		}
		replication.setTargetRedisClientOptions(targetRedisClientOptions());
		replication.setTtlTolerance(Duration.ofMillis(ttlTolerance));
		replication.setType(type ? ReplicationType.STRUCT : ReplicationType.DUMP);
		replication.setWriterOptions(targetRedisWriterArgs.writerOptions());
		replication.setFlushInterval(Duration.ofMillis(flushInterval));
		replication.setIdleTimeout(Duration.ofMillis(idleTimeout));
		replication.setNotificationQueueCapacity(sourceRedisReaderArgs.getNotificationQueueCapacity());
		replication.setRedisClientOptions(sourceRedisClientArgs.redisClientOptions());
		replication.setReaderOptions(sourceRedisReaderArgs.redisReaderOptions());
		replication.setProcessorOptions(processorArgs.processorOptions());
		return replication;
	}

	private RedisClientOptions targetRedisClientOptions() {
		RedisClientOptions options = targetRedisClientArgs.redisClientOptions();
		options.setSslOptions(sourceRedisClientArgs.getSslArgs().sslOptions());
		return options;
	}

	@Override
	protected String taskName(String stepName) {
		return taskNames.getOrDefault(stepName, "Unknown");
	}

	private static String compareMessageFormat() {
		StringBuilder builder = new StringBuilder();
		for (Status status : STATUSES) {
			builder.append(String.format(" | %s: %s", status.name().toLowerCase(), NUMBER_FORMAT));
		}
		return builder.toString();
	}

	@Override
	protected Supplier<String> extraMessage(String stepName, ItemReader<?> reader, ItemWriter<?> writer) {
		switch (stepName) {
		case Replication.STEP_COMPARE:
			return () -> compareExtraMessage(writer);
		case Replication.STEP_LIVE:
			return () -> liveExtraMessage((RedisItemReader<?, ?, ?>) reader);
		default:
			return super.extraMessage(stepName, reader, writer);
		}
	}

	private String compareExtraMessage(ItemWriter<?> writer) {
		return String.format(COMPARE_MESSAGE,
				((KeyComparisonStatusCountItemWriter) writer).getCounts(STATUSES).toArray());
	}

	private String liveExtraMessage(RedisItemReader<?, ?, ?> reader) {
		KeyNotificationItemReader<?, ?> keyReader = (KeyNotificationItemReader<?, ?>) reader.getReader();
		if (keyReader == null || keyReader.getQueue() == null) {
			return "";
		}
		return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity());
	}

	public void setIdleTimeout(long timeout) {
		this.idleTimeout = timeout;
	}

	public ReplicationMode getMode() {
		return mode;
	}

	public void setMode(ReplicationMode mode) {
		this.mode = mode;
	}

	public boolean isType() {
		return type;
	}

	public void setType(boolean type) {
		this.type = type;
	}

	public long getTtlTolerance() {
		return ttlTolerance;
	}

	public void setTtlTolerance(long tolerance) {
		this.ttlTolerance = tolerance;
	}

	public boolean isShowDiffs() {
		return showDiffs;
	}

	public void setShowDiffs(boolean showDiffs) {
		this.showDiffs = showDiffs;
	}

	public CompareMode getCompareMode() {
		return compareMode;
	}

	public void setCompareMode(CompareMode mode) {
		this.compareMode = mode;
	}

	public long getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(long interval) {
		this.flushInterval = interval;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public KeyValueProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueProcessorArgs args) {
		this.processorArgs = args;
	}

	public RedisWriterArgs getTargetRedisWriterArgs() {
		return targetRedisWriterArgs;
	}

	public void setTargetRedisWriterArgs(RedisWriterArgs args) {
		this.targetRedisWriterArgs = args;
	}

	public RedisClientArgs getSourceRedisClientArgs() {
		return sourceRedisClientArgs;
	}

	public void setSourceRedisClientArgs(RedisClientArgs args) {
		this.sourceRedisClientArgs = args;
	}

	public ReplicateRedisReaderArgs getSourceRedisReaderArgs() {
		return sourceRedisReaderArgs;
	}

	public void setSourceRedisReaderArgs(ReplicateRedisReaderArgs args) {
		this.sourceRedisReaderArgs = args;
	}

	public ReplicateTargetRedisClientArgs getTargetRedisClientArgs() {
		return targetRedisClientArgs;
	}

	public void setTargetRedisClientArgs(ReplicateTargetRedisClientArgs args) {
		this.targetRedisClientArgs = args;
	}

}
