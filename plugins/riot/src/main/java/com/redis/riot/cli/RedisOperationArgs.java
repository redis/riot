package com.redis.riot.cli;

import java.time.Duration;

import com.redis.riot.core.RedisOperationOptions;

import picocli.CommandLine.Option;

public class RedisOperationArgs {

    @Option(names = "--multi-exec", description = "Enable MULTI/EXEC writes.")
    private boolean multiExec;

    @Option(names = "--wait-replicas", description = "Number of replicas for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int waitReplicas;

    @Option(names = "--wait-timeout", description = "Timeout in millis for WAIT command (default: ${DEFAULT-VALUE}).", paramLabel = "<ms>")
    private long waitTimeout = RedisOperationOptions.DEFAULT_WAIT_TIMEOUT.toMillis();

    @Option(names = "--write-pool", description = "Max connections for writer pool (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
    private int poolSize = RedisOperationOptions.DEFAULT_POOL_SIZE;

    public RedisOperationOptions operationOptions() {
        RedisOperationOptions options = new RedisOperationOptions();
        configure(options);
        return options;
    }

    protected void configure(RedisOperationOptions options) {
        options.setMultiExec(multiExec);
        options.setPoolSize(poolSize);
        options.setWaitReplicas(waitReplicas);
        options.setWaitTimeout(Duration.ofMillis(waitTimeout));
    }

}
