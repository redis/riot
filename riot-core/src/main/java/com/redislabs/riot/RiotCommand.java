package com.redislabs.riot;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(abbreviateSynopsis = true, sortOptions = false)
public class RiotCommand extends HelpCommand {

    @ParentCommand
    private RiotApp app;

    protected String toString(RedisURI redisURI) {
	if (redisURI.getSocket() != null) {
	    return redisURI.getSocket();
	}
	if (redisURI.getSentinelMasterId() != null) {
	    return redisURI.getSentinelMasterId();
	}
	return redisURI.getHost();
    }

    protected RedisURI redisURI() {
	return app.getRedisConnectionOptions().redisURI();
    }

    public <B extends RedisConnectionBuilder<String, String, B>> B configure(B builder) throws Exception {
	return configure(builder, app.getRedisConnectionOptions());
    }

    protected <B extends RedisConnectionBuilder<String, String, B>> B configure(B builder,
	    RedisConnectionOptions options) throws Exception {
	builder.uri(options.redisURI()).cluster(options.isCluster()).clientResources(options.clientResources())
		.clientOptions(options.clientOptions()).poolConfig(options.poolConfig());
	GenericObjectPool<StatefulConnection<String, String>> pool = builder.pool();
	try (StatefulConnection<String, String> connection = pool.borrowObject()) {
	}
	return builder;
    }

}
