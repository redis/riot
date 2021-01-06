package com.redislabs.riot;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
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
    private AbstractRedisClient client;

    protected RedisClusterClient getRedisClusterClient() {
        return (RedisClusterClient) client;
    }

    protected RedisClient getRedisClient() {
        return (RedisClient) client;
    }

    protected RedisURI getRedisURI() {
        return app.getRedisOptions().uri();
    }

    protected Duration getCommandTimeout() {
        return getRedisURI().getTimeout();
    }

    protected boolean isCluster() {
        return app.getRedisOptions().isCluster();
    }

    protected GenericObjectPool<StatefulRedisConnection<String, String>> redisPool() {
        return app.getRedisOptions().pool(getRedisClient());
    }

    protected GenericObjectPool<StatefulRedisClusterConnection<String, String>> redisClusterPool() {
        return app.getRedisOptions().pool(getRedisClusterClient());
    }

    @Override
    public void run() {
        try {
            execute();
        } catch (Exception e) {
            log.error("Could not execute command", e);
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.client = app.getRedisOptions().client();
    }

    public void execute() throws Exception {
        afterPropertiesSet();
        try {
            doExecute();
        } finally {
            shutdown();
        }
    }

    protected void shutdown() {
        client.shutdown();
        client.getResources().shutdown();
    }

    protected StatefulRedisConnection<String, String> redisConnection() {
        return getRedisClient().connect();
    }

    protected StatefulRedisClusterConnection<String, String> redisClusterConnection() {
        return getRedisClusterClient().connect();
    }

    protected StatefulConnection<String, String> connection() {
        if (isCluster()) {
            return redisClusterConnection();
        }
        return redisConnection();
    }

    protected BaseRedisCommands<String, String> sync() {
        if (isCluster()) {
            return getRedisClusterClient().connect().sync();
        }
        return getRedisClient().connect().sync();
    }

    protected BaseRedisAsyncCommands<String, String> async() {
        if (isCluster()) {
            return getRedisClusterClient().connect().async();
        }
        return getRedisClient().connect().async();
    }

    protected abstract void doExecute() throws Exception;

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
