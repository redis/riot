package com.redis.riot.cli;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.cli.RedisReaderArgs.ReadFromEnum;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.redis.CompareMode;
import com.redis.riot.redis.KeyComparisonStatusCountItemWriter;
import com.redis.riot.redis.Replication;
import com.redis.riot.redis.ReplicationMode;
import com.redis.riot.redis.ReplicationType;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.KeyComparison.Status;
import com.redis.spring.batch.reader.KeyComparisonItemReader;
import com.redis.spring.batch.reader.KeyNotificationItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class ReplicateCommand extends AbstractExportCommand {

	private static final Status[] STATUSES = { Status.OK, Status.MISSING, Status.TYPE, Status.VALUE, Status.TTL };
	private static final String QUEUE_MESSAGE = " | queue capacity: %,d";
	private static final String NUMBER_FORMAT = "%,d";
	private static final String COMPARE_MESSAGE = compareMessageFormat();
	private static final Map<String, String> taskNames = taskNames();

	@Option(names = "--mode", description = "Replication mode (default: ${DEFAULT-VALUE}):%n  SNAPSHOT: initial replication using key scan.%n  LIVE: initial and continuous replication using key scan and keyspace notifications in parallel.%n  LIVEONLY: continuous replication using keyspace notifications (only changed keys are replicated).%n  COMPARE: compare source and target database.", paramLabel = "<name>")
	ReplicationMode mode = ReplicationMode.SNAPSHOT;

	@Option(names = "--type", description = "Enable type-based replication")
	boolean type;

	@Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
	long ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE.toMillis();

	@Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
	boolean showDiffs;

	@Option(names = "--compare", description = "Comparison mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<mode>")
	CompareMode compareMode = Replication.DEFAULT_COMPARE_MODE;

	@ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
	RedisArgs targetRedisArgs = new RedisArgs();

	@Option(names = "--target-read-from", description = "Which target Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
	ReadFromEnum targetReadFrom;

	@ArgGroup(exclusive = false, heading = "Writer options%n")
	RedisWriterArgs writerArgs = new RedisWriterArgs();

	private static Map<String, String> taskNames() {
		Map<String, String> map = new HashMap<>();
		map.put(Replication.STEP_SCAN, "Scanning");
		map.put(Replication.STEP_LIVE, "Listening");
		map.put(Replication.STEP_COMPARE, "Comparing");
		return map;
	}

	@Override
	protected AbstractExport exportRunnable() {
		Replication replication = new Replication();
		replication.setCompareMode(compareMode);
		replication.setMode(mode);
		replication.setShowDiffs(showDiffs);
		if (targetReadFrom != null) {
			replication.setTargetReadFrom(targetReadFrom.getReadFrom());
		}
		replication.setTargetRedisClientOptions(targetRedisArgs.redisOptions());
		replication.setTtlTolerance(Duration.ofMillis(ttlTolerance));
		replication.setType(type ? ReplicationType.STRUCT : ReplicationType.DUMP);
		replication.setWriterOptions(writerArgs.writerOptions());
		return replication;
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
	protected Supplier<String> extraMessageSupplier(String stepName, ItemReader<?> reader, ItemWriter<?> writer) {
		switch (stepName) {
		case Replication.STEP_COMPARE:
			return () -> compareExtraMessage(writer);
		case Replication.STEP_LIVE:
			return () -> liveExtraMessage((RedisItemReader<?, ?, ?>) reader);
		default:
			return super.extraMessageSupplier(stepName, reader, writer);
		}
	}

	private String compareExtraMessage(ItemWriter<?> writer) {
		return String.format(COMPARE_MESSAGE,
				((KeyComparisonStatusCountItemWriter) writer).getCounts(STATUSES).toArray());
	}

	private String liveExtraMessage(RedisItemReader<?, ?, ?> reader) {
		KeyNotificationItemReader<?, ?> keyReader = (KeyNotificationItemReader<?, ?>) reader.getReader();
		if (keyReader == null || keyReader.getQueue() == null) {
			return ProgressStepExecutionListener.EMPTY_STRING;
		}
		return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity());
	}

}
