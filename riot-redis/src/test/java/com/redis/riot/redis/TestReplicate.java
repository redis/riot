package com.redis.riot.redis;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.DatabaseComparator;
import org.springframework.batch.item.redis.LiveKeyDumpItemReader;
import org.springframework.batch.item.redis.support.DataStructureReader;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.MultiTransferExecution;
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

	@Container
	private static final GenericContainer targetRedis = redisContainer();
	private RedisURI targetRedisURI;
	private RedisClient targetClient;
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
		DataStructureItemReader sourceReader = DataStructureItemReader.builder().client(client).build();
		DataStructureReader targetReader = DataStructureReader.builder().client(targetClient).build();
		DatabaseComparator comparator = DatabaseComparator.builder().left(sourceReader).right(targetReader).build();
		comparator.execution().start().get();
		Assert.assertEquals(Math.toIntExact(sync.dbsize()), comparator.getOk().size());
	}

	@Override
	protected String process(String command) {
		String processedCommand = command.replace("-h source -p 6379", "").replace("-h target -p 6380",
				connectionArgs(targetRedis));
		return super.process(processedCommand);
	}

	@Test
	public void replicateLive() throws Exception {
		sync.configSet("notify-keyspace-events", "AK");
		DataGenerator.builder().client(client).build().run();
		ReplicateCommand command = (ReplicateCommand) command("/replicate-live.txt");
		MultiTransferExecution execution = command.execution();
		execution.start();
		Thread.sleep(400);
		int count = 39;
		for (int index = 0; index < count; index++) {
			sync.set("livestring:" + index, "value" + index);
			Thread.sleep(1);
		}
		Thread.sleep(100);
		LiveKeyDumpItemReader reader = (LiveKeyDumpItemReader) execution.getExecutions().get(1).getTransfer()
				.getReader();
		LiveKeyItemReader keyReader = (LiveKeyItemReader) reader.getKeyReader();
		keyReader.stop();
		Long sourceSize = sync.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		Long targetSize = targetSync.dbsize();
		Assertions.assertEquals(sourceSize, targetSize);
	}
}
