package com.redislabs.riot;

import com.redislabs.mesclun.RedisModulesClient;
import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.InitializingBean;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

import java.util.concurrent.Callable;

@Slf4j
@Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class RiotCommand extends HelpCommand implements InitializingBean, Callable<Integer> {

    @SuppressWarnings("unused")
    @ParentCommand
    protected RiotApp app;
    protected AbstractRedisClient client;
    protected GenericObjectPool<? extends StatefulConnection<String, String>> pool;
    protected StatefulConnection<String, String> connection;

    protected RedisURI getRedisURI() {
        return app.getRedisOptions().uris().get(0);
    }

    @Getter
    @Setter
    private boolean executeAsync;

    @Override
    public Integer call() throws Exception {
        afterPropertiesSet();
        try {
            return execute();
        } finally {
            if (!executeAsync) {
                shutdown();
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.client = app.getRedisOptions().client();
        this.pool = pool(app.getRedisOptions(), client);
        this.connection = RedisOptions.connection(client);
    }

    protected boolean isCluster() {
        return app.getRedisOptions().isCluster();
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

    protected BaseRedisCommands<String, String> sync() {
        if (connection instanceof StatefulRedisClusterConnection) {
            return ((StatefulRedisClusterConnection<String, String>) connection).sync();
        }
        return ((StatefulRedisConnection<String, String>) connection).sync();
    }

    protected GenericObjectPool<? extends StatefulConnection<String, String>> pool(RedisOptions redisOptions, AbstractRedisClient client) {
        if (client instanceof RedisClusterClient) {
            return ConnectionPoolSupport.createGenericObjectPool(((RedisClusterClient) client)::connect, redisOptions.poolConfig());
        }
        return ConnectionPoolSupport.createGenericObjectPool(((RedisModulesClient) client)::connect, redisOptions.poolConfig());
    }

    protected abstract int execute() throws Exception;

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
