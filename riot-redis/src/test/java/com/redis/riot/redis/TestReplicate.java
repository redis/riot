package com.redis.riot.redis;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import com.redislabs.riot.RiotApp;
import com.redislabs.riot.redis.ReplicateCommand;
import com.redislabs.riot.redis.RiotRedis;
import com.redislabs.riot.test.BaseTest;
import com.redislabs.riot.test.DataGenerator;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

@SuppressWarnings({ "rawtypes" })
public class TestReplicate extends BaseTest {

    private final static Logger log = LoggerFactory.getLogger(TestReplicate.class);

    @Container
    private static final GenericContainer targetRedis = redisContainer();
    private RedisClient targetClient;

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
	RedisURI targetRedisURI = redisURI(targetRedis);
	targetClient = RedisClient.create(targetRedisURI);
	targetClient.connect().sync().flushall();
    }

    @AfterEach
    public void teardownTarget() {
	if (targetClient != null) {
	    targetClient.shutdown();
	}
    }

    @Test
    public void replicate() throws Exception {
	targetClient.connect().sync().flushall();
	DataGenerator.builder().connection(connection()).build().run();
	Long sourceSize = commands().dbsize();
	Assertions.assertTrue(sourceSize > 0);
	executeFile("/replicate.txt");
	Assertions.assertEquals(sourceSize, targetClient.connect().sync().dbsize());
    }

    @Override
    protected String process(String command) {
	String processedCommand = command.replace("-h source -p 6379", "").replace("-h target -p 6380",
		connectionArgs(targetRedis));
	return super.process(processedCommand);
    }

    @Test
    public void replicateLive() throws Exception {
	StatefulRedisConnection<String, String> connection = connection();
	connection.sync().configSet("notify-keyspace-events", "AK");
	connection.close();
	StatefulRedisConnection<String, String> targetConnection = targetClient.connect();
	targetConnection.sync().flushall();
	DataGenerator.builder().connection(connection()).build().run();
	ReplicateCommand command = (ReplicateCommand) command("/replicate-live.txt");
	CompletableFuture<Void> future = command.executeAsync();
	Thread.sleep(400);
	RedisCommands<String, String> commands = commands();
	int count = 39;
	for (int index = 0; index < count; index++) {
	    commands.set("livestring:" + index, "value" + index);
	    Thread.sleep(1);
	}
	Thread.sleep(200);
	log.info("Stopping transfer");
	future.cancel(true);
	Long sourceSize = commands.dbsize();
	Assertions.assertTrue(sourceSize > 0);
	Long targetSize = targetConnection.sync().dbsize();
	Assertions.assertEquals(sourceSize, targetSize);
	targetConnection.close();
    }
}
