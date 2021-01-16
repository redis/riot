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
import org.springframework.beans.factory.InitializingBean;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.time.Duration;

@Slf4j
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class RiotCommand extends HelpCommand implements InitializingBean {

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

    protected Duration getCommandTimeout() {
        return getRedisURI().getTimeout();
    }

    @Override
    public void run() {
        try {
            afterPropertiesSet();
        } catch (Exception e) {
            log.error("Could not initialize command", e);
            return;
        }
        try {
            execute();
        } catch (Exception e) {
            log.error("Could not execute command", e);
        } finally {
            shutdown();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.client = client(app.getRedisOptions());
        this.pool = pool(app.getRedisOptions(), client);
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
