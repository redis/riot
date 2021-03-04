package com.redislabs.riot;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.redis.support.CommandTimeoutBuilder;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.ClassUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.time.Duration;
import java.util.concurrent.Callable;

@Slf4j
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class RiotCommand extends HelpCommand implements InitializingBean, Callable<Integer> {

    @SuppressWarnings("unused")
    @ParentCommand
    private RiotApp app;
    protected AbstractRedisClient client;
    protected GenericObjectPool<? extends StatefulConnection<String, String>> pool;
    protected StatefulConnection<String, String> connection;

    protected boolean isCluster() {
        return app.getRedisOptions().isCluster();
    }

    protected RedisURI getRedisURI() {
        return app.getRedisOptions().uris().get(0);
    }

    protected <B extends CommandTimeoutBuilder<B>> B configureCommandTimeoutBuilder(B builder) {
        Duration commandTimeout = getRedisURI().getTimeout();
        log.info("Configuring {} with command timeout {}", ClassUtils.getShortName(builder.getClass()), commandTimeout);
        return builder.commandTimeout(commandTimeout);
    }

    @Override
    public Integer call() throws Exception {
        afterPropertiesSet();
        try {
            execute();
            return 0;
        } finally {
            shutdown();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.client = client(app.getRedisOptions());
        this.pool = pool(app.getRedisOptions(), client);
        log.info("Connecting to {}", app.getRedisOptions().uris());
        this.connection = connection(client);
    }

    public void shutdown() {
        if (connection != null) {
            connection.close();
        }
        if (pool != null) {
            pool.close();
        }
        if (client != null) {
            client.shutdown();
            client.getResources().shutdown();
        }
    }

    protected AbstractRedisClient client(RedisOptions redisOptions) {
        if (redisOptions.isCluster()) {
            return redisOptions.redisClusterClient();
        }
        return redisOptions.redisClient();
    }

    protected BaseRedisCommands<String, String> sync() {
        if (connection instanceof StatefulRedisClusterConnection) {
            return ((StatefulRedisClusterConnection<String, String>) connection).sync();
        }
        return ((StatefulRedisConnection<String, String>) connection).sync();
    }


    protected StatefulConnection<String, String> connection(AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            return ((RedisClusterClient) client).connect();
        }
        return ((RedisClient) client).connect();
    }

    protected GenericObjectPool<? extends StatefulConnection<String, String>> pool(RedisOptions redisOptions, AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            return ConnectionPoolSupport.createGenericObjectPool(((RedisClusterClient) client)::connect, redisOptions.poolConfig());
        }
        return ConnectionPoolSupport.createGenericObjectPool(((RedisClient) client)::connect, redisOptions.poolConfig());
    }

    protected abstract void execute() throws Exception;

    protected String name(RedisURI redisURI) {
        if (redisURI.getSocket() != null) {
            return redisURI.getSocket();
        }
        if (redisURI.getSentinelMasterId() != null) {
            return redisURI.getSentinelMasterId();
        }
        return redisURI.getHost();
    }

}
