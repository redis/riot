package com.redis.riot.cli;

import java.time.Duration;
import java.util.function.Supplier;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.EvaluationContextOptions;
import com.redis.riot.core.KeyComparisonStatusCountItemWriter;
import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.Replication;
import com.redis.riot.core.ReplicationMode;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.reader.KeyspaceNotificationItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class ReplicationCommand extends AbstractExportCommand {

    private static final String COMPARE_MESSAGE = " | %,d missing | %,d type | %,d value | %,d ttl";

    private static final String QUEUE_MESSAGE = " | %,d queue capacity";

    private static final String VAR_SOURCE_REDIS_URI = "src";

    private static final String VAR_TARGET_REDIS_URI = "tgt";

    @Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    ReplicationMode mode = ReplicationMode.SNAPSHOT;

    @Option(names = "--type", description = "Replication strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    ValueType valueType = ValueType.DUMP;

    @Option(names = "--no-verify", description = "Disable verifying target against source dataset after replication.")
    boolean noVerify;

    @Option(names = "--ttl-tolerance", description = "Max TTL offset in millis to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    long ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE.toMillis();

    @Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
    boolean showDiffs;

    @ArgGroup(exclusive = false, heading = "Processor options%n")
    KeyValueProcessorArgs processorArgs = new KeyValueProcessorArgs();

    @ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    RedisArgs targetRedisArgs = new RedisArgs();

    @ArgGroup(exclusive = false, heading = "Target Redis reader options%n")
    ReplicationTargetReaderArgs targetReaderArgs = new ReplicationTargetReaderArgs();

    @ArgGroup(exclusive = false, heading = "Target Redis writer options%n")
    RedisWriterArgs targetWriterArgs = new RedisWriterArgs();

    @Override
    protected AbstractExport getExportExecutable() {
        Replication executable = new Replication(redisClient(), targetRedisArgs.client(), parent.out);
        executable.setProcessorOptions(processorOptions());
        executable.setMode(mode);
        executable.setNoVerify(noVerify);
        executable.setProcessorOptions(processorOptions());
        executable.setShowDiff(showDiffs);
        executable.setTargetReaderOptions(targetReaderArgs.readerOptions());
        executable.setTargetWriterOptions(targetWriterArgs.writerOptions());
        executable.setTtlTolerance(Duration.ofMillis(ttlTolerance));
        executable.setValueType(valueType);
        executable.setReaderOptions(readerOptions());
        return executable;
    }

    private KeyValueProcessorOptions processorOptions() {
        KeyValueProcessorOptions options = processorArgs.keyValueOperatorOptions();
        EvaluationContextOptions evaluationContextOptions = options.getEvaluationContextOptions();
        evaluationContextOptions.addVariable(VAR_SOURCE_REDIS_URI, redisArgs().uri());
        evaluationContextOptions.addVariable(VAR_TARGET_REDIS_URI, targetRedisArgs.uri());
        return options;
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
        KeyComparisonStatusCountItemWriter writer = (KeyComparisonStatusCountItemWriter) step.getWriter();
        return () -> String.format(COMPARE_MESSAGE, writer.getMissing(), writer.getType(), writer.getValue(), writer.getTtl());
    }

}
