package com.redis.riot.redis;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.redis.riot.AbstractRiotIntegrationTests;
import com.redis.riot.redis.ReplicationOptions.ReplicationMode;
import com.redis.spring.batch.reader.DataStructureGeneratorItemReader;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import com.redis.testcontainers.junit.RedisTestContext;
import com.redis.testcontainers.junit.RedisTestContextsSource;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.SlotHash;
import picocli.CommandLine;

@Testcontainers
@SuppressWarnings("unchecked")
class RiotRedisIntegrationTests extends AbstractRiotIntegrationTests {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(3);

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
		AbstractReplicateCommand<?> command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		command.getTargetRedisOptions().setUri(RedisURI.create(targetRedis.getRedisURI()));
		ReplicationMode mode = command.getReplicationOptions().getMode();
		if (mode == ReplicationMode.LIVE || mode == ReplicationMode.LIVEONLY) {
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
		generate(DataStructureGeneratorItemReader.builder().maxItemCount(200).build(), redis);
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
	void replicateLiveKeySlot(RedisTestContext source) throws Exception {
		source.sync().configSet("notify-keyspace-events", "AK");
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.schedule(() -> {
			try {
				generate(1, DataStructureGeneratorItemReader.builder().maxItemCount(300).build(), source);
			} catch (Exception e) {
				log.error("Could not generate data", e);
			}
		}, 500, TimeUnit.MILLISECONDS);
		execute("replicate-live-keyslot", source, this::configureReplicateCommand);
		List<String> keys = getContext(targetRedis).sync().keys("*");
		for (String key : keys) {
			int slot = SlotHash.getSlot(key);
			Assertions.assertTrue(slot >= 0 && slot <= 8000);
		}
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
		source.sync().configSet("notify-keyspace-events", "AK");
		generate(DataStructureGeneratorItemReader.builder().maxItemCount(3000).build(), source);
		ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
		executor.schedule(() -> {
			DataStructureGeneratorItemReader reader = DataStructureGeneratorItemReader.builder().currentItemCount(3000)
					.maxItemCount(5000).build();
			try {
				generate(1, reader, source);
			} catch (Exception e) {
				log.error("Could not generate data", e);
			}
		}, 500, TimeUnit.MILLISECONDS);
		execute(filename, source, this::configureReplicateCommand);
	}

}
