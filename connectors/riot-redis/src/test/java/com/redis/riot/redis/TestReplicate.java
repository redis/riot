package com.redis.riot.redis;

import com.redis.riot.AbstractRiotIntegrationTest;
import com.redis.riot.AbstractRiotCommand;
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

import java.time.Duration;
import java.time.Instant;

@Testcontainers
@Slf4j
@SuppressWarnings({"rawtypes", "unchecked", "BusyWait"})
public class TestReplicate extends AbstractRiotIntegrationTest {

    private final static Duration REPLICATION_TIMEOUT = Duration.ofSeconds(5);

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

    private void configureReplicateCommand(CommandLine.ParseResult parseResult, boolean async) {
        AbstractReplicateCommand command = parseResult.subcommand().commandSpec().commandLine().getCommand();
        if (async) {
            command.setExecutionStrategy(AbstractRiotCommand.ExecutionStrategy.ASYNC);
        }
        command.getTargetRedisOptions().setUris(new RedisURI[]{RedisURI.create(TARGET.getRedisURI())});
        if (command.getReplicationOptions().getMode() == ReplicationMode.LIVE) {
            command.getFlushingTransferOptions().setIdleTimeout(Duration.ofMillis(300));
        }
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("containers")
    void replicate(RedisServer container) throws Throwable {
        dataGenerator(container).build().call();
        RedisServerCommands<String, String> sync = sync(container);
        Long sourceSize = sync.dbsize();
        Assertions.assertTrue(sourceSize > 0);
        execute("replicate", container, r -> configureReplicateCommand(r, false));
        Assertions.assertEquals(sourceSize, targetSync.dbsize());
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("containers")
    void replicateKeyProcessor(RedisServer container) throws Throwable {
        dataGenerator(container).build().call();
        RedisServerCommands<String, String> sync = sync(container);
        Long sourceSize = sync.dbsize();
        Assertions.assertTrue(sourceSize > 0);
        execute("replicate-key-processor", container, r -> configureReplicateCommand(r, false));
        Assertions.assertEquals(sourceSize, targetSync.dbsize());
        RedisStringCommands<String, String> stringCommands = sync(container);
        Assertions.assertEquals(stringCommands.get("string:123"), targetSync.get("0:string:123"));
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("containers")
    public void replicateLive(RedisServer container) throws Exception {
        testLiveReplication(container, "replicate-live");
    }

    @ParameterizedTest(name = "{displayName} - {index}: {0}")
    @MethodSource("containers")
    public void replicateLiveValue(RedisServer container) throws Exception {
        testLiveReplication(container, "replicate-live-value");
    }

    private void testLiveReplication(RedisServer container, String filename) throws Exception {
        dataGenerator(container).build().call();
        execute(filename, container, r -> configureReplicateCommand(r, true));
        while (targetSync.dbsize() < 100) {
            Thread.sleep(10);
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
