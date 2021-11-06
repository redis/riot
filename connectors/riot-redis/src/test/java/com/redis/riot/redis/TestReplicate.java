package com.redis.riot.redis;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.riot.AbstractRiotIntegrationTest;
import com.redis.riot.redis.ReplicationOptions.ReplicationMode;
import com.redis.spring.batch.support.compare.KeyComparisonResults;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import picocli.CommandLine;

@Testcontainers
@SuppressWarnings("unchecked")
class TestReplicate extends AbstractRiotIntegrationTest {

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(10);

	@Container
	private static final RedisContainer TARGET = new RedisContainer();

	private RedisClient targetClient;
	private StatefulRedisConnection<String, String> targetConnection;
	private RedisCommands<String, String> targetSync;

	@BeforeEach
	public void setupTarget() {
		targetClient = RedisClient.create(TARGET.getRedisURI());
		targetConnection = targetClient.connect();
		targetSync = targetConnection.sync();
	}

	@AfterEach
	public void cleanupTarget() throws InterruptedException {
		targetConnection.sync().flushall();
		targetConnection.close();
		targetClient.shutdown();
		targetClient.getResources().shutdown();
	}

	@Override
	protected RiotRedis app() {
		return new RiotRedis();
	}

	private void configureReplicateCommand(CommandLine.ParseResult parseResult) {
		ReplicateCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		command.getTargetRedisOptions().setUris(new RedisURI[] { RedisURI.create(TARGET.getRedisURI()) });
		if (command.getReplicationOptions().getMode() == ReplicationMode.LIVE) {
			command.getFlushingTransferOptions().setIdleTimeout(IDLE_TIMEOUT);
			command.getReplicationOptions().setNotificationQueueCapacity(100000);
		}
	}

	@ParameterizedTest
	@MethodSource("containers")
	public void replicate(RedisServer redis) throws Throwable {
		String name = "replicate";
		execute(dataGenerator(redis, name));
		RedisModulesCommands<String, String> sync = sync(redis);
		Assertions.assertTrue(sync.dbsize() > 0);
		execute("replicate", redis, this::configureReplicateCommand);
		compare(name, redis);
	}

	@ParameterizedTest
	@MethodSource("containers")
	public void replicateKeyProcessor(RedisServer redis) throws Throwable {
		RedisModulesCommands<String, String> sync = sync(redis);
		execute(dataGenerator(redis, "replicate-key-processor").end(200));
		Long sourceSize = sync.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute("replicate-key-processor", redis, this::configureReplicateCommand);
		Assertions.assertEquals(sourceSize, targetSync.dbsize());
		Assertions.assertEquals(sync.get("string:123"), targetSync.get("0:string:123"));
	}

	@ParameterizedTest
	@MethodSource("containers")
	public void replicateLive(RedisServer container) throws Exception {
		runLiveReplication("replicate-live", container);
	}

	@ParameterizedTest
	@MethodSource("containers")
	public void replicateDSLive(RedisServer container) throws Exception {
		runLiveReplication("replicate-ds-live", container);
	}

	private void runLiveReplication(String filename, RedisServer source) throws Exception {
		execute(dataGenerator(source, filename).end(3000));
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(() -> {
			try {
				Thread.sleep(500);
				dataGenerator(source, "live-" + filename).chunkSize(1).between(3000, 5000).build();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
		execute(filename, source, this::configureReplicateCommand);
		compare(filename, source);
	}

	private void compare(String name, RedisServer redis) throws Exception {
		RedisServerCommands<String, String> sourceSync = sync(redis);
		Assertions.assertEquals(sourceSync.dbsize(), targetSync.dbsize());
		KeyComparisonResults results = keyComparator(client(redis), targetClient).id(name).build().call();
		Assertions.assertTrue(results.isOK());
	}

}
