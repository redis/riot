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

import io.lettuce.core.RedisURI;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

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

	@ArgGroup(exclusive = false, heading = "Redis TLS options%n")
	private SslArgs sslArgs = new SslArgs();

	@Parameters(arity = "1", description = "Source Redis server URI.", paramLabel = "SOURCE")
	private RedisURI sourceRedisURI;

	@ArgGroup(exclusive = false, heading = "Source Redis options%n")
	private ReplicateSourceRedisArgs sourceRedisArgs = new ReplicateSourceRedisArgs();

	@ArgGroup(exclusive = false, heading = "Processor options%n")
	private KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

	@Parameters(arity = "1", description = "Target Redis server URI.", paramLabel = "TARGET")
	private RedisURI targetRedisURI;

	@ArgGroup(exclusive = false, heading = "Target Redis options%n")
	private ReplicateTargetRedisArgs targetRedisArgs = new ReplicateTargetRedisArgs();

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
		replication.setFlushInterval(Duration.ofMillis(flushInterval));
		replication.setIdleTimeout(Duration.ofMillis(idleTimeout));
		replication.setMode(mode);
		replication.setNotificationQueueCapacity(sourceRedisArgs.getReaderArgs().getNotificationQueueCapacity());
		replication.setRedisURI(sourceRedisArgs.redisURI(sourceRedisURI));
		replication.setRedisClientOptions(sourceRedisClientOptions());
		replication.setReaderOptions(sourceRedisArgs.getReaderArgs().redisReaderOptions());
		replication.setProcessorOptions(processorArgs.processorOptions());
		replication.setShowDiffs(showDiffs);
		if (targetRedisArgs.getReadFrom() != null) {
			replication.setTargetReadFrom(targetRedisArgs.getReadFrom().getReadFrom());
		}
		replication.setTargetRedisURI(targetRedisArgs.redisURI(targetRedisURI));
		replication.setTargetRedisClientOptions(targetRedisClientOptions());
		replication.setTtlTolerance(Duration.ofMillis(ttlTolerance));
		replication.setType(type ? ReplicationType.STRUCT : ReplicationType.DUMP);
		replication.setWriterOptions(targetRedisArgs.getWriterArgs().writerOptions());
		return replication;
	}

	private RedisClientOptions sourceRedisClientOptions() {
		RedisClientOptions options = sourceRedisArgs.redisClientOptions();
		options.setSslOptions(sslArgs.sslOptions());
		return options;
	}

	private RedisClientOptions targetRedisClientOptions() {
		RedisClientOptions options = targetRedisArgs.redisClientOptions();
		options.setSslOptions(sslArgs.sslOptions());
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

	public ReplicateSourceRedisArgs getSourceRedisArgs() {
		return sourceRedisArgs;
	}

	public void setSourceRedisArgs(ReplicateSourceRedisArgs args) {
		this.sourceRedisArgs = args;
	}

	public RedisURI getTargetRedisURI() {
		return targetRedisURI;
	}

	public void setTargetRedisURI(RedisURI targetRedisURI) {
		this.targetRedisURI = targetRedisURI;
	}

	public ReplicateTargetRedisArgs getTargetRedisArgs() {
		return targetRedisArgs;
	}

	public void setTargetRedisArgs(ReplicateTargetRedisArgs args) {
		this.targetRedisArgs = args;
	}

	public RedisURI getSourceRedisURI() {
		return sourceRedisURI;
	}

	public void setSourceRedisURI(RedisURI sourceRedisURI) {
		this.sourceRedisURI = sourceRedisURI;
	}

	public SslArgs getSslArgs() {
		return sslArgs;
	}

	public void setSslArgs(SslArgs sslArgs) {
		this.sslArgs = sslArgs;
	}

}
