package com.redis.riot.cli;

import com.redis.riot.cli.RedisReaderArgs.ReadFromEnum;
import com.redis.riot.core.RedisOperationOptions;
import com.redis.riot.core.RedisReaderOptions;

import picocli.CommandLine.Option;

public class ReplicationTargetReaderArgs {

    @Option(names = "--target-read-pool", description = "Max connections for target Redis pool (default: ${DEFAULT-VALUE}).", paramLabel = "<n>")
    int poolSize = RedisOperationOptions.DEFAULT_POOL_SIZE;

    @Option(names = "--target-read-from", description = "Which target Redis cluster nodes to read data from: ${COMPLETION-CANDIDATES}.", paramLabel = "<n>")
    ReadFromEnum readFrom;

    public RedisReaderOptions readerOptions() {
        RedisReaderOptions options = new RedisReaderOptions();
        options.setPoolSize(poolSize);
        if (readFrom != null) {
            options.setReadFrom(readFrom.getValue());
        }
        return options;
    }

}
