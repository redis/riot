package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.RedisWriterOptions;

import picocli.CommandLine.Option;

public class RedisWriterArgs {

    @Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
    private boolean multiExec;

    @Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int waitReplicas;

    @Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    private long waitTimeout = RedisWriterOptions.DEFAULT_WAIT_TIMEOUT.toMillis();

    @Option(names = "--write-pool", description = "Max connections for writer pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int poolSize = RedisWriterOptions.DEFAULT_POOL_SIZE;

    @Option(names = "--merge", description = "Merge properties from collection data structures (`hash`, `set`, ...) instead of overwriting them.")
    private boolean merge;

    public RedisWriterOptions writerOptions() {
        RedisWriterOptions options = new RedisWriterOptions();
        options.setMultiExec(multiExec);
        options.setPoolSize(poolSize);
        options.setWaitReplicas(waitReplicas);
        options.setWaitTimeout(Duration.ofMillis(waitTimeout));
        options.setMerge(merge);
        return options;
    }

}
