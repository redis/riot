package com.redislabs.riot.test;

import com.redislabs.riot.RedisOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
@SuppressWarnings("rawtypes")
public abstract class AbstractStandaloneRedisTest extends AbstractRiotTest {

    private static final DockerImageName DOCKER_IMAGE_NAME = DockerImageName.parse("redislabs/redisearch:latest");

    protected RedisURI redisURI;
    protected RedisClient client;
    protected GenericObjectPool<StatefulRedisConnection<String, String>> pool;
    protected StatefulRedisConnection<String, String> connection;
    protected RedisCommands<String, String> sync;
    protected RedisAsyncCommands<String, String> async;

    @Container
    protected static final GenericContainer redis = redisContainer();

    @SuppressWarnings("resource")
    protected static GenericContainer redisContainer() {
        return new GenericContainer(DOCKER_IMAGE_NAME).withExposedPorts(REDIS_PORT);
    }

    @BeforeEach
    public void setup() {
        redisURI = redisURI();
        client = RedisClient.create(redisURI);
        pool = ConnectionPoolSupport.createGenericObjectPool(client::connect, new GenericObjectPoolConfig<>());
        connection = client.connect();
        sync = connection.sync();
        async = connection.async();
        sync.flushall();
    }

    protected RedisURI redisURI() {
        return RedisURI.create(getRedisHost(), getRedisPort());
    }

    @Override
    protected String connectionArgs() {
        return "-h " + getRedisHost() + " -p " + getRedisPort();
    }

    @AfterEach
    public void teardown() {
        connection.close();
        client.shutdown();
    }

    protected RedisOptions redisOptions() {
        RedisOptions redisOptions = new RedisOptions();
        redisOptions.setHost(redisURI.getHost());
        redisOptions.setPort(redisURI.getPort());
        return redisOptions;
    }

    protected String connectionArgs(String redisHost, int redisPort) {
        return "-h " + redisHost + " -p " + redisPort;
    }

    protected int getRedisPort() {
        return redis.getFirstMappedPort();
    }

    protected String getRedisHost() {
        return redis.getHost();
    }


}
