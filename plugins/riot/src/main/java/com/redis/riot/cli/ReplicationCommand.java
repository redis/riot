package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.AbstractJobExecutable;
import com.redis.riot.core.EvaluationContextOptions;
import com.redis.riot.core.KeyValueOperatorOptions;
import com.redis.riot.core.RedisOperationOptions;
import com.redis.riot.core.RedisReaderOptions;
import com.redis.riot.core.replicate.Replication;
import com.redis.riot.core.replicate.ReplicationMode;
import com.redis.spring.batch.ValueType;
import com.redis.spring.batch.util.KeyComparisonItemReader;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "replicate", description = "Replicate a Redis database into another Redis database.")
public class ReplicationCommand extends AbstractJobCommand {

    // private static final String COMPARE_MESSAGE = " | {0} missing | {1} type | {2} value | {3} ttl";
    //
    // private static final String QUEUE_MESSAGE = " | {0} queued";

    private static final String VAR_SOURCE_REDIS_URI = "src";

    private static final String VAR_TARGET_REDIS_URI = "tgt";

    @Option(names = "--mode", description = "Replication mode: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private ReplicationMode mode = ReplicationMode.SNAPSHOT;

    @Option(names = "--type", description = "Replication strategy: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<name>")
    private ValueType valueType = ValueType.DUMP;

    @Option(names = "--no-verify", description = "Disable verifying target against source dataset after replication.")
    private boolean noVerify;

    @Option(names = "--ttl-tolerance", description = "Max TTL difference to use for dataset verification (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    private long ttlTolerance = KeyComparisonItemReader.DEFAULT_TTL_TOLERANCE.toMillis();

    @Option(names = "--show-diffs", description = "Print details of key mismatches during dataset verification. Disables progress reporting.")
    private boolean showDiffs; // TODO

    @ArgGroup(exclusive = false, heading = "Source Redis reader options%n")
    private RedisReaderArgs sourceReaderArgs = new RedisReaderArgs();

    @ArgGroup(exclusive = false, heading = "Processor options%n")
    private KeyValueOperatorArgs processorArgs = new KeyValueOperatorArgs();

    @ArgGroup(exclusive = false, heading = "Target Redis connection options%n")
    private RedisArgs targetRedisArgs = new RedisArgs();

    @ArgGroup(exclusive = false, heading = "Target Redis reader options%n")
    private TargetReaderArgs targetReaderArgs = new TargetReaderArgs();

    @ArgGroup(exclusive = false, heading = "Target Redis writer options%n")
    private RedisWriterArgs targetWriterArgs = new RedisWriterArgs();

    public ReplicationMode getMode() {
        return mode;
    }

    public RedisReaderArgs getSourceReaderArgs() {
        return sourceReaderArgs;
    }

    public TargetReaderArgs getTargetReaderArgs() {
        return targetReaderArgs;
    }

    public RedisArgs getTargetRedisArgs() {
        return targetRedisArgs;
    }

    @Override
    protected AbstractJobExecutable getJobExecutable() {
        Replication executable = new Replication(redisClient(), targetRedisArgs.client());
        executable.setProcessorOptions(processorOptions());
        executable.setMode(mode);
        executable.setNoVerify(noVerify);
        executable.setProcessorOptions(processorOptions());
        executable.setTargetWriterOptions(targetWriterArgs.redisWriterOptions());
        executable.setTargetReaderOptions(targetRedisReaderOptions());
        executable.setTargetWriterOptions(targetWriterArgs.redisWriterOptions());
        executable.setTtlTolerance(Duration.ofMillis(ttlTolerance));
        executable.setValueType(valueType);
        RedisReaderOptions redisReaderOptions = sourceReaderArgs.redisReaderOptions();
        redisReaderOptions.setDatabase(redisArgs().uri().getDatabase());
        executable.setRedisReaderOptions(redisReaderOptions);
        return executable;
    }

    private KeyValueOperatorOptions processorOptions() {
        KeyValueOperatorOptions options = processorArgs.keyValueOperatorOptions();
        EvaluationContextOptions evaluationContextOptions = options.getEvaluationContextOptions();
        evaluationContextOptions.addVariable(VAR_SOURCE_REDIS_URI, redisArgs().uri());
        evaluationContextOptions.addVariable(VAR_TARGET_REDIS_URI, targetRedisArgs.uri());
        return options;
    }

    private RedisReaderOptions targetRedisReaderOptions() {
        RedisReaderOptions options = new RedisReaderOptions();
        options.setPoolSize(targetReaderArgs.poolSize);
        if (targetReaderArgs.readFrom != null) {
            options.setReadFrom(targetReaderArgs.readFrom.getValue());
        }
        return options;
    }

    private static class TargetReaderArgs {

        @Option(names = "--target-read-pool", description = "Max connections for target Redis pool (default: ${DEFAULT-VALUE}).", paramLabel = "<n>")
        private int poolSize = RedisOperationOptions.DEFAULT_POOL_SIZE;

        @Option(names = "--target-read-from", description = "Which target Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
        private ReadFromEnum readFrom;

    }

}
