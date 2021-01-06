package com.redis.riot.redis;

import com.redislabs.riot.RiotApp;
import com.redislabs.riot.redis.ReplicateCommand;
import com.redislabs.riot.redis.RiotRedis;
import com.redislabs.riot.test.BaseTest;
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
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.support.DatabaseComparator;
import org.springframework.batch.item.redis.support.DatabaseComparison;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import java.util.concurrent.CompletableFuture;

@Slf4j
@SuppressWarnings({"rawtypes"})
public class TestReplicate extends BaseTest {

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
    protected String applicationName() {
        return "riot-redis";
    }

    @BeforeEach
    public void setupTarget() {
        targetRedisURI = redisURI(targetRedis);
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
        DataGenerator.builder().client(client).build().run();
        Long sourceSize = sync.dbsize();
        Assertions.assertTrue(sourceSize > 0);
        executeFile("/replicate.txt");
        Assertions.assertEquals(sourceSize, targetSync.dbsize());
        RedisDataStructureItemReader<String, String> left = RedisDataStructureItemReader.builder(pool, connection).build();
        RedisDataStructureItemReader<String, String> right = RedisDataStructureItemReader.builder(targetPool, targetConnection).build();
        DatabaseComparator<String> comparator = DatabaseComparator.<String>builder().left(left).right(right).build();
        DatabaseComparison<String> comparison = comparator.execute();
        Assertions.assertTrue(comparison.isIdentical());
    }

    @Override
    protected String process(String command) {
        String processedCommand = command.replace("-h source -p 6379", "").replace("-h target -p 6380", connectionArgs(targetRedis));
        return super.process(processedCommand);
    }

    @Test
    public void replicateLive() throws Exception {
        sync.configSet("notify-keyspace-events", "AK");
        DataGenerator.builder().client(client).build().run();
        ReplicateCommand command = (ReplicateCommand) command("/replicate-live.txt");
        CompletableFuture<Void> future = CompletableFuture.runAsync(command);
        long dbsize;
        do {
            dbsize = targetSync.dbsize();
            Thread.sleep(100);
        } while (dbsize < 2000);
        int count = 39;
        for (int index = 0; index < count; index++) {
            sync.set("livestring:" + index, "value" + index);
            Thread.sleep(1);
        }
        Thread.sleep(500);
        future.cancel(true);
        Long sourceSize = sync.dbsize();
        Assertions.assertTrue(sourceSize > 0);
        Long targetSize = targetSync.dbsize();
        Assertions.assertEquals(sourceSize, targetSize);
    }
}
