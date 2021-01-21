package com.redis.riot.redis;

import com.redislabs.riot.RiotApp;
import com.redislabs.riot.redis.ReplicateCommand;
import com.redislabs.riot.redis.RiotRedis;
import com.redislabs.riot.test.AbstractStandaloneRedisTest;
import com.redislabs.riot.test.DataGenerator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

@Slf4j
@SuppressWarnings({"rawtypes"})
public class TestReplicate extends AbstractStandaloneRedisTest {

    @Container
    private static final GenericContainer targetRedis = redisContainer();
    private RedisURI targetRedisURI;
    private RedisClient targetClient;
    private GenericObjectPool<StatefulRedisConnection<String, String>> targetPool;
    private StatefulRedisConnection<String, String> targetConnection;
    private RedisCommands<String, String> targetSync;

    @Override
    protected RiotApp app() {
        return new RiotRedis();
    }

    @Override
    protected String appName() {
        return "riot-redis";
    }

    @BeforeEach
    public void setupTarget() {
        targetRedisURI = RedisURI.create(targetRedis.getHost(), targetRedis.getFirstMappedPort());
        targetClient = RedisClient.create(targetRedisURI);
        targetPool = ConnectionPoolSupport.createGenericObjectPool(targetClient::connect, new GenericObjectPoolConfig<>());
        targetConnection = targetClient.connect();
        targetSync = targetConnection.sync();
        targetSync.flushall();
    }

    @AfterEach
    public void teardownTarget() {
        targetSync = null;
        targetConnection.close();
        targetClient.shutdown();
    }

    @Test
    public void replicate() throws Exception {
        targetSync.flushall();
        DataGenerator.builder().commands(async).build().run();
        Long sourceSize = sync.dbsize();
        Assertions.assertTrue(sourceSize > 0);
        executeFile("/replicate.txt");
        Assertions.assertEquals(sourceSize, targetSync.dbsize());
    }

    @Override
    protected String process(String command) {
        String processedCommand = command.replace("-h source -p 6379", "").replace("-h target -p 6380", connectionArgs(targetRedis.getHost(), targetRedis.getFirstMappedPort()));
        return super.process(processedCommand);
    }

    @Test
    public void replicateLive() throws Exception {
        sync.configSet("notify-keyspace-events", "AK");
        DataGenerator.builder().commands(async).build().run();
        ReplicateCommand command = (ReplicateCommand) command("/replicate-live.txt");
        JobExecution execution = command.executeAsync();
        Thread.sleep(100);
        log.info("Setting livestring keys");
        int count = 39;
        for (int index = 0; index < count; index++) {
            sync.set("livestring:" + index, "value" + index);
        }
        Thread.sleep(100);
        awaitTermination(execution);
        Assertions.assertEquals(sync.dbsize(), targetSync.dbsize());
    }
}
