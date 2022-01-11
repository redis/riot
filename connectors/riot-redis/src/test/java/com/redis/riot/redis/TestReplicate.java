package com.redis.riot.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.riot.AbstractRiotIntegrationTests;
import com.redis.riot.redis.ReplicationOptions.ReplicationMode;
import com.redis.spring.batch.support.compare.KeyComparisonResults;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.RedisURI;
import picocli.CommandLine;

@Testcontainers
@SuppressWarnings("unchecked")
class TestReplicate extends AbstractRiotIntegrationTests {

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(10);

	private final RedisContainer targetRedis = new RedisContainer(
			RedisContainer.DEFAULT_IMAGE_NAME.withTag(RedisContainer.DEFAULT_TAG));

	@Override
	protected Collection<RedisTestContext> getRedisTestContexts() {
		Collection<RedisTestContext> redisTestContexts = new ArrayList<>(super.getRedisTestContexts());
		redisTestContexts.removeIf(c -> c.getServer() == targetRedis);
		return redisTestContexts;
	}

	@Override
	protected Collection<RedisServer> redisServers() {
		Collection<RedisServer> servers = new ArrayList<>(super.redisServers());
		servers.add(targetRedis);
		return servers;
	}

	@Override
	protected RiotRedis app() {
		return new RiotRedis();
	}

	private void configureReplicateCommand(CommandLine.ParseResult parseResult) {
		ReplicateCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		command.getTargetRedisOptions().setUris(new RedisURI[] { RedisURI.create(targetRedis.getRedisURI()) });
		if (command.getReplicationOptions().getMode() == ReplicationMode.LIVE) {
			command.getFlushingTransferOptions().setIdleTimeout(IDLE_TIMEOUT);
			command.getReplicationOptions().setNotificationQueueCapacity(100000);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicate(RedisTestContext redis) throws Throwable {
		String name = "replicate";
		execute(dataGenerator(redis, name));
		Assertions.assertTrue(redis.sync().dbsize() > 0);
		execute("replicate", redis, this::configureReplicateCommand);
		compare(name, redis);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateKeyProcessor(RedisTestContext redis) throws Throwable {
		execute(dataGenerator(redis, "replicate-key-processor").end(200));
		Long sourceSize = redis.sync().dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute("replicate-key-processor", redis, this::configureReplicateCommand);
		RedisTestContext target = getRedisTestContext(targetRedis);
		Assertions.assertEquals(sourceSize, target.sync().dbsize());
		Assertions.assertEquals(redis.sync().get("string:123"), target.sync().get("0:string:123"));
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateLive(RedisTestContext container) throws Exception {
		runLiveReplication("replicate-live", container);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateLiveThreads(RedisTestContext container) throws Exception {
		runLiveReplication("replicate-live-threads", container);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateDSLive(RedisTestContext container) throws Exception {
		runLiveReplication("replicate-ds-live", container);
	}

	private void runLiveReplication(String filename, RedisTestContext source) throws Exception {
		execute(dataGenerator(source, filename).end(3000));
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(() -> {
			try {
				Thread.sleep(500);
				dataGenerator(source, "live-" + filename).chunkSize(1).between(3000, 5000).build();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		});
		execute(filename, source, this::configureReplicateCommand);
		compare(filename, source);
	}

	private void compare(String name, RedisTestContext redis) throws Exception {
		Assertions.assertEquals(redis.sync().dbsize(), getRedisTestContext(targetRedis).sync().dbsize());
		KeyComparisonResults results = keyComparator(redis, getRedisTestContext(targetRedis)).id(name).build().call();
		Assertions.assertTrue(results.isOK());
	}

}
