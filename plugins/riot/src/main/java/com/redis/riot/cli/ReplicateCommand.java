package com.redis.riot.cli;

import java.time.Duration;
import java.util.function.Supplier;

import com.redis.riot.cli.RedisReaderArgs.ReadFromEnum;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.KeyComparisonOptions;
import com.redis.riot.core.KeyComparisonStatusCountItemWriter;
import com.redis.riot.core.Replication;
import com.redis.riot.core.ReplicationMode;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.KeyComparison.Status;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class ReplicateCommand extends AbstractExportCommand<byte[], byte[]> {

    private static final Status[] STATUSES = { Status.OK, Status.MISSING, Status.TYPE, Status.VALUE, Status.TTL };

    private static final String QUEUE_MESSAGE = " | %,d queue capacity";

    private static final String NUMBER_FORMAT = "%,d";

    private static final String COMPARE_MESSAGE = " | %s: %s";

    @Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    ReplicationMode mode = ReplicationMode.SNAPSHOT;

    @Option(names = "--type", description = "Replication strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    ValueType valueType = ValueType.DUMP;

    @ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    RedisArgs targetRedisClientArgs = new RedisArgs();

    @Option(names = "--target-read-from", description = "Which target Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
    ReadFromEnum targetReadFrom;

    @ArgGroup(exclusive = false, heading = "Target writer options%n")
    RedisWriterArgs targetWriterArgs = new RedisWriterArgs();

    @ArgGroup(exclusive = false, heading = "Compare options%n")
    ComparisonArgs compareArgs = new ComparisonArgs();

    private static class ComparisonArgs {

        @Option(names = "--no-verify", description = "Disable comparing target against source after replication.")
        boolean noVerify;

        @Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
        long ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE.toMillis();

        @Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
        boolean showDiffs;

        public KeyComparisonOptions comparisonOptions() {
            KeyComparisonOptions options = new KeyComparisonOptions();
            options.setNoVerify(noVerify);
            options.setShowDiff(showDiffs);
            options.setTtlTolerance(Duration.ofMillis(ttlTolerance));
            return options;
        }

    }

    @Override
    protected AbstractExport<byte[], byte[]> getExportExecutable() {
        Replication executable = new Replication(parent.out);
        executable.setComparisonOptions(compareArgs.comparisonOptions());
        executable.setMode(mode);
        executable.setReaderOptions(readerOptions());
        executable.setTargetRedisClientOptions(targetRedisClientArgs.redisClientOptions());
        if (targetReadFrom != null) {
            executable.setTargetReadFrom(targetReadFrom.getReadFrom());
        }
        executable.setTargetWriterOptions(targetWriterArgs.writerOptions());
        executable.setValueType(valueType);
        return executable;
    }

    @Override
    protected String taskName(StepBuilder<?, ?> step) {
        switch (step.getName()) {
            case Replication.STEP_SCAN:
                return "Scanning";
            case Replication.STEP_LIVE:
                return "Listening";
            case Replication.STEP_COMPARE:
                return "Comparing";
            default:
                return "Unknown";
        }
    }

    @Override
    protected long size(StepBuilder<?, ?> step) {
        if (Replication.STEP_LIVE.equals(step.getName())) {
            return BatchUtils.SIZE_UNKNOWN;
        }
        return super.size(step);
    }

    @Override
    protected Supplier<String> extraMessage(StepBuilder<?, ?> step) {
        switch (step.getName()) {
            case Replication.STEP_COMPARE:
                return compareMessage(step);
            case Replication.STEP_LIVE:
                return liveMessage(step);
            default:
                return super.extraMessage(step);
        }
    }

    private Supplier<String> liveMessage(StepBuilder<?, ?> step) {
        RedisItemReader<?, ?> reader = (RedisItemReader<?, ?>) step.getReader();
        return () -> liveMessage(reader);
    }

    private String liveMessage(RedisItemReader<?, ?> reader) {
        KeyspaceNotificationItemReader<?, ?> keyReader = (KeyspaceNotificationItemReader<?, ?>) reader.getKeyReader();
        if (keyReader == null) {
            return "";
        }
        return String.format(QUEUE_MESSAGE, keyReader.getQueue().remainingCapacity());

    }

    private Supplier<String> compareMessage(StepBuilder<?, ?> step) {
        String format = compareFormat();
        return () -> String.format(format, counts(step));
    }

    private String compareFormat() {
        StringBuilder builder = new StringBuilder();
        for (Status status : STATUSES) {
            builder.append(String.format(COMPARE_MESSAGE, status.name().toLowerCase(), NUMBER_FORMAT));
        }
        return builder.toString();
    }

    private Object[] counts(StepBuilder<?, ?> step) {
        return ((KeyComparisonStatusCountItemWriter) step.getWriter()).getCounts(STATUSES);
    }

}
