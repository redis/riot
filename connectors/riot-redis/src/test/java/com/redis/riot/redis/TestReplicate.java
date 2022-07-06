package com.redis.riot.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.riot.AbstractRiotIntegrationTests;
import com.redis.riot.redis.ReplicationOptions.ReplicationMode;
import com.redis.spring.batch.support.RandomDataStructureItemReader;
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
	protected Collection<RedisServer> testRedisServers() {
		return super.redisServers();
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
		command.getTargetRedisOptions().setUri(RedisURI.create(targetRedis.getRedisURI()));
		if (command.getReplicationOptions().getMode() == ReplicationMode.LIVE) {
			command.getFlushingTransferOptions().setIdleTimeout(IDLE_TIMEOUT);
			command.getReplicationOptions().setNotificationQueueCapacity(100000);
		}
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicate(RedisTestContext redis) throws Throwable {
		generate(redis);
		Assertions.assertTrue(redis.sync().dbsize() > 0);
		execute("replicate", redis, this::configureReplicateCommand);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateDryRun(RedisTestContext redis) throws Throwable {
		generate(redis);
		Assertions.assertTrue(redis.sync().dbsize() > 0);
		execute("replicate-dry-run", redis, this::configureReplicateCommand);
		Assertions.assertEquals(0, getContext(targetRedis).sync().dbsize());
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateHyperLogLog(RedisTestContext redis) throws Throwable {
		String key = "crawled:20171124";
		String value = "http://www.google.com/";
		redis.sync().pfadd(key, value);
		Assertions.assertTrue(redis.sync().dbsize() > 0);
		execute("replicate-hll", redis, this::configureReplicateCommand);
	}

	@ParameterizedTest
	@RedisTestContextsSource
	void replicateKeyProcessor(RedisTestContext redis) throws Throwable {
		generate(RandomDataStructureItemReader.builder().end(200).build(), redis);
		Long sourceSize = redis.sync().dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute("replicate-key-processor", redis, this::configureReplicateCommand);
		RedisTestContext target = getContext(targetRedis);
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
		generate(RandomDataStructureItemReader.builder().end(3000).build(), source);
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.schedule(() -> {
			try {
				generate(1, RandomDataStructureItemReader.builder().between(3000, 5000).build(), source);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}, 500, TimeUnit.MILLISECONDS);
		execute(filename, source, this::configureReplicateCommand);
	}

}
