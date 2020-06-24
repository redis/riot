package com.redislabs.riot;

import io.lettuce.core.RedisURI;
import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine;

@Getter
public class RedisConnectionOptions {

    @CommandLine.Option(names = {"-r", "--redis"}, description = "Redis connection string (default: redis://localhost:6379)", paramLabel = "<uri>")
    private RedisURI redisURI = RedisURI.create("localhost", RedisURI.DEFAULT_REDIS_PORT);
    @CommandLine.Option(names = {"-c", "--cluster"}, description = "Connect to a Redis Cluster")
    private boolean cluster;
    @CommandLine.Option(names = {"-m", "--metrics"}, description = "Show metrics")
    private boolean showMetrics;
    @CommandLine.Option(names = {"-p", "--pool"}, description = "Max pool connections (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int poolMaxTotal = GenericObjectPoolConfig.DEFAULT_MAX_TOTAL;

}
