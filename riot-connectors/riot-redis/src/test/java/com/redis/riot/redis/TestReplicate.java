package com.redis.riot.redis;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.redis.support.KeyDump;
import org.springframework.batch.item.redis.support.LiveKeyItemReader;
import org.springframework.batch.item.redis.support.RedisItemReader;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import com.redislabs.riot.Transfer;
import com.redislabs.riot.redis.ReplicateCommand;
import com.redislabs.riot.redis.RiotRedis;
import com.redislabs.riot.test.BaseTest;
import com.redislabs.riot.test.DataPopulator;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import picocli.CommandLine;

@SuppressWarnings({ "rawtypes" })
public class TestReplicate extends BaseTest {

	private final static Logger log = LoggerFactory.getLogger(TestReplicate.class);

	@Container
	private static final GenericContainer targetRedis = redisContainer();
	private RedisClient targetClient;

	@Override
	protected int execute(String[] args) {
		return new RiotRedis().execute(args);
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
		DataPopulator.builder().connection(connection()).build().run();
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

	@SuppressWarnings("unchecked")
	@Test
	public void replicateLive() throws Exception {
		StatefulRedisConnection<String, String> connection = connection();
		connection.sync().configSet("notify-keyspace-events", "AK");
		connection.close();
		StatefulRedisConnection<String, String> targetConnection = targetClient.connect();
		targetConnection.sync().flushall();
		DataPopulator.builder().connection(connection()).build().run();
		String[] commandArgs = args("/replicate-live.txt");
		RiotRedis riotRedis = new RiotRedis();
		CommandLine commandLine = riotRedis.commandLine();
		CommandLine.ParseResult parseResult = commandLine.parseArgs(commandArgs);
		ReplicateCommand command = parseResult.asCommandLineList().get(1).getCommand();
		Transfer<KeyDump<String>, KeyDump<String>> transfer = command.transfers().get(0);
		CompletableFuture<Void> future = command.execute(transfer);
		Thread.sleep(400);
		RedisCommands<String, String> commands = commands();
		int count = 39;
		for (int index = 0; index < count; index++) {
			commands.set("livestring:" + index, "value" + index);
			Thread.sleep(1);
		}
		Thread.sleep(200);
		RedisItemReader<String, KeyDump<String>> reader = (RedisItemReader<String, KeyDump<String>>) transfer
				.getReader();
		LiveKeyItemReader<String, String> keyReader = (LiveKeyItemReader<String, String>) reader.getKeyReader();
		log.info("Stopping LiveKeyItemReader");
		keyReader.stop();
		Long sourceSize = commands.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		Long targetSize = targetConnection.sync().dbsize();
		Assertions.assertEquals(sourceSize, targetSize);
		targetConnection.close();
		future.cancel(false);
	}
}
