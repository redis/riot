package com.redis.riot.redis;

import com.redislabs.riot.RiotApp;
import com.redislabs.riot.redis.ReplicateCommand;
import com.redislabs.riot.redis.RiotRedis;
import com.redislabs.riot.test.AbstractRiotTest;
import com.redislabs.riot.test.DataGenerator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.JobExecution;
import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Testcontainers
@SuppressWarnings({"rawtypes"})
public class TestReplicateCluster extends AbstractRiotTest {

    private static final String REDIS_CLUSTER_DOCKER_IMAGE_NAME = "grokzen/redis-cluster:latest";
    private static final String REDIS_DOCKER_IMAGE_NAME = "redis:latest";

    @Container
    @SuppressWarnings("deprecation")
    private static GenericContainer sourceRedis = new FixedHostPortGenericContainer(REDIS_CLUSTER_DOCKER_IMAGE_NAME).withFixedExposedPort(7000, 7000).withFixedExposedPort(7001, 7001).withFixedExposedPort(7002, 7002).withFixedExposedPort(7003, 7003).withFixedExposedPort(7004, 7004).withFixedExposedPort(7005, 7005).withEnv("IP", "0.0.0.0").waitingFor(Wait.forListeningPort());
    @Container
    @SuppressWarnings("deprecation")
    private static GenericContainer targetRedis = new GenericContainer(REDIS_DOCKER_IMAGE_NAME).withExposedPorts(REDIS_PORT);

    private List<RedisURI> sourceURIs;
    private RedisClusterClient sourceClient;
    private StatefulRedisClusterConnection<String, String> sourceConnection;
    private RedisURI targetURI;
    private RedisClient targetClient;
    private StatefulRedisConnection<String, String> targetConnection;

    @Override
    protected RiotApp app() {
        return new RiotRedis();
    }

    @Override
    protected String appName() {
        return "riot-redis";
    }

    @Override
    protected String connectionArgs() {
        return "-u " + String.join(" ", sourceURIs.stream().map(String::valueOf).collect(Collectors.toList()));
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        // wait enough time for all cluster nodes to start and become ready
        log.info("Waiting 10 sec for cluster to become ready");
        Thread.sleep(10000);
        String redisIpAddress = "0.0.0.0";
        sourceURIs = IntStream.rangeClosed(7000, 7005).boxed().map(p -> RedisURI.create(redisIpAddress, p)).collect(Collectors.toList());
        sourceClient = RedisClusterClient.create(sourceURIs);
        log.info("Connecting to source cluster");
        sourceConnection = sourceClient.connect();
        log.info("Flushing source db");
        sourceConnection.sync().flushall();
        IntStream.rangeClosed(7000, 7005).forEach(p -> RedisClient.create(RedisURI.create(redisIpAddress, p)).connect().sync().configSet("notify-keyspace-events", "AK"));
        targetURI = RedisURI.create(targetRedis.getHost(), targetRedis.getFirstMappedPort());
        targetClient = RedisClient.create(targetURI);
        log.info("Connecting to target");
        targetConnection = targetClient.connect();
        log.info("Flushing target db");
        targetConnection.sync().flushall();
    }


    @AfterEach
    public void teardown() {
        sourceConnection.close();
        sourceClient.shutdown();
        targetConnection.close();
        targetClient.shutdown();
    }

    @Override
    protected String process(String command) {
        String processedCommand = command.replace("-h target -p 6380", "-h " + targetRedis.getHost() + " -p " + targetRedis.getFirstMappedPort());
        return super.process(processedCommand);
    }

    @Test
    public void replicateLive() throws Exception {
        DataGenerator.builder().commands(sourceConnection.sync()).end(10000).build().run();
        ReplicateCommand command = (ReplicateCommand) command("/replicate-cluster-live.txt");
        JobExecution execution = command.executeAsync();
        while (!execution.isRunning()) {
            Thread.sleep(10);
        }
        int count = 39;
        for (int index = 0; index < count; index++) {
            sourceConnection.sync().set("livestring:" + index, "value" + index);
            Thread.sleep(1);
        }
        long sourceSize = sourceConnection.sync().dbsize();
        do {
            Thread.sleep(100);
        } while (targetConnection.sync().dbsize() < sourceSize);
        execution.stop();
        command.shutdown();
        Assertions.assertEquals(sourceSize, targetConnection.sync().dbsize());
    }

}
