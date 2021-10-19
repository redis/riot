package com.redis.riot.redis;

import com.redis.riot.AbstractRiotIntegrationTest;
import com.redis.riot.AbstractRiotCommand;
import com.redis.riot.AbstractRiotCommand.ExecutionStrategy;
import com.redis.testcontainers.RedisContainer;
import com.redis.testcontainers.RedisServer;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import picocli.CommandLine;
import picocli.CommandLine.ParseResult;

import java.time.Duration;
import java.time.Instant;

@Testcontainers
@Slf4j
@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestReplicate extends AbstractRiotIntegrationTest {

	private final static Duration REPLICATION_TIMEOUT = Duration.ofSeconds(10);

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
	public void cleanupTarget() {
		targetConnection.sync().flushall();
		targetConnection.close();
		targetClient.shutdown();
		targetClient.getResources().shutdown();
	}

	@Override
	protected RiotRedis app() {
		return new RiotRedis();
	}
	
	private void configureReplicateCommandAsync(ParseResult parseResult) {
		configureReplicateCommand(parseResult, ExecutionStrategy.ASYNC);
	}

	private void configureReplicateCommandSync(CommandLine.ParseResult parseResult) {
		configureReplicateCommand(parseResult, ExecutionStrategy.SYNC);
	}

	private void configureReplicateCommand(CommandLine.ParseResult parseResult,
			AbstractRiotCommand.ExecutionStrategy strategy) {
		AbstractReplicateCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
		if (strategy == ExecutionStrategy.ASYNC) {
			command.setExecutionStrategy(AbstractRiotCommand.ExecutionStrategy.ASYNC);
		}
		command.getTargetRedisOptions().setUris(new RedisURI[] { RedisURI.create(TARGET.getRedisURI()) });
		if (command.getReplicationOptions().getMode() == ReplicationMode.LIVE) {
			command.getFlushingTransferOptions().setIdleTimeout(Duration.ofMillis(300));
		}
	}

	@ParameterizedTest
	@MethodSource("containers")
	void replicate(RedisServer container) throws Throwable {
		dataGenerator(container).build().call();
		RedisServerCommands<String, String> sync = sync(container);
		Long sourceSize = sync.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute("replicate", container, this::configureReplicateCommandSync);
		Assertions.assertEquals(sourceSize, targetSync.dbsize());
	}

	@ParameterizedTest
	@MethodSource("containers")
	void replicateKeyProcessor(RedisServer container) throws Throwable {
		dataGenerator(container).build().call();
		RedisServerCommands<String, String> sync = sync(container);
		Long sourceSize = sync.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute("replicate-key-processor", container, this::configureReplicateCommandSync);
		Assertions.assertEquals(sourceSize, targetSync.dbsize());
		RedisStringCommands<String, String> stringCommands = sync(container);
		Assertions.assertEquals(stringCommands.get("string:123"), targetSync.get("0:string:123"));
	}

	@ParameterizedTest
	@MethodSource("containers")
	public void replicateLive(RedisServer container) throws Exception {
		testLiveReplication(container, "replicate-live");
	}

	@ParameterizedTest
	@MethodSource("containers")
	public void replicateLiveValue(RedisServer container) throws Exception {
		testLiveReplication(container, "replicate-live-value");
	}

	private void testLiveReplication(RedisServer container, String filename) throws Exception {
		dataGenerator(container).build().call();
		execute(filename, container, this::configureReplicateCommandAsync);
		while (targetSync.dbsize() < 300) {
			Thread.sleep(1);
		}
		RedisStringCommands<String, String> sync = sync(container);
		log.debug("Setting livestring keys");
		int count = 39;
		for (int index = 0; index < count; index++) {
			sync.set("livestring:" + index, "value" + index);
		}
		Instant start = Instant.now();
		long sourceSize = ((RedisServerCommands<String, String>) sync).dbsize();
		while (targetSync.dbsize() != sourceSize && isActive(start, REPLICATION_TIMEOUT)) {
			Thread.sleep(10);
		}
		Assertions.assertEquals(sourceSize, targetSync.dbsize());
	}

	private boolean isActive(Instant start, Duration timeout) {
		return Duration.between(start, Instant.now()).compareTo(timeout) < 0;
	}
}
