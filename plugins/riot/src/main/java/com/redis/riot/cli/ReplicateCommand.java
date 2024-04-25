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

import io.lettuce.core.ClientOptions;
import io.lettuce.core.cluster.ClusterClientOptions;
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

	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private ReplicateSourceArgs sourceArgs = new ReplicateSourceArgs();

	@ArgGroup(exclusive = false, heading = "Target Redis options%n")
	private ReplicateTargetArgs targetArgs = new ReplicateTargetArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

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
		if (targetArgs.getReadFrom() != null) {
			replication.setTargetReadFrom(targetArgs.getReadFrom().getReadFrom());
		}
		replication.setTargetRedisClientOptions(targetRedisClientOptions());
		replication.setTtlTolerance(Duration.ofMillis(ttlTolerance));
		replication.setType(type ? ReplicationType.STRUCT : ReplicationType.DUMP);
		replication.setWriterOptions(targetArgs.getWriterArgs().writerOptions());
		replication.setFlushInterval(Duration.ofMillis(flushInterval));
		replication.setIdleTimeout(Duration.ofMillis(idleTimeout));
		replication.setNotificationQueueCapacity(sourceArgs.getNotificationQueueCapacity());
		replication.setRedisClientOptions(sourceArgs.getRedisClientArgs().redisClientOptions());
		replication.setReaderOptions(sourceArgs.getRedisReaderArgs().redisReaderOptions());
		replication.setProcessorOptions(processorArgs.processorOptions());
		return replication;
	}

	private RedisClientOptions targetRedisClientOptions() {
		RedisClientOptions options = new RedisClientOptions();
		options.setRedisURI(targetArgs.redisURI());
		options.setCluster(targetArgs.isCluster());
		options.setOptions(targetClientOptions());
		return options;
	}

	private ClientOptions targetClientOptions() {
		if (targetArgs.isCluster()) {
			ClusterClientOptions.Builder options = ClusterClientOptions.builder();
			configure(options);
			return options.build();
		}
		ClientOptions.Builder options = ClientOptions.builder();
		configure(options);
		return options.build();
	}

	private void configure(ClientOptions.Builder builder) {
		builder.autoReconnect(targetArgs.isAutoReconnect());
		builder.protocolVersion(targetArgs.getProtocolVersion());
		builder.sslOptions(sourceArgs.getRedisClientArgs().getSslArgs().sslOptions());
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

	public void setTtlTolerance(long ttlTolerance) {
		this.ttlTolerance = ttlTolerance;
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

	public void setCompareMode(CompareMode compareMode) {
		this.compareMode = compareMode;
	}

	public long getFlushInterval() {
		return flushInterval;
	}

	public void setFlushInterval(long flushInterval) {
		this.flushInterval = flushInterval;
	}

	public long getIdleTimeout() {
		return idleTimeout;
	}

	public ReplicateSourceArgs getSourceArgs() {
		return sourceArgs;
	}

	public void setSourceArgs(ReplicateSourceArgs sourceArgs) {
		this.sourceArgs = sourceArgs;
	}

	public ReplicateTargetArgs getTargetArgs() {
		return targetArgs;
	}

	public void setTargetArgs(ReplicateTargetArgs targetArgs) {
		this.targetArgs = targetArgs;
	}

	public KeyValueProcessorArgs getProcessorArgs() {
		return processorArgs;
	}

	public void setProcessorArgs(KeyValueProcessorArgs processorArgs) {
		this.processorArgs = processorArgs;
	}

}
