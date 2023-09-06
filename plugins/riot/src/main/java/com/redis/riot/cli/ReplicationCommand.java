package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.EvaluationContextOptions;
import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.Replication;
import com.redis.riot.core.ReplicationMode;
import com.redis.riot.core.StepBuilder;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class ReplicationCommand extends AbstractExportCommand {

    // private static final String COMPARE_MESSAGE = " | {0} missing | {1} type | {2} value | {3} ttl";
    //
    // private static final String QUEUE_MESSAGE = " | {0} queued";

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
        Replication executable = new Replication(redisClient(), targetRedisArgs.client());
        executable.setProcessorOptions(processorOptions());
        executable.setMode(mode);
        executable.setNoVerify(noVerify);
        executable.setProcessorOptions(processorOptions());
        executable.setShowDiff(showDiffs);
        executable.setOut(parent.out);
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
        if (Replication.STEP_COMPARE.equals(step.getName())) {
            return BatchUtils.SIZE_UNKNOWN;
        }
        return super.size(step);
    }

}
