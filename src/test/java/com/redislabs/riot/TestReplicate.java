package com.redislabs.riot;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import redis.embedded.RedisServer;

@Slf4j
public class TestReplicate extends BaseTest {

    private final static String TARGET_HOST = "localhost";
    private final static int TARGET_PORT = 6380;

    @Test
    public void testReplicate() throws Exception {
        RedisServer target = serverBuilder(TARGET_PORT).build();
        try {
            target.start();
            runFile("gen-faker");
            Long sourceSize = commands().dbsize();
            Assertions.assertTrue(sourceSize > 0);
            runFile("replicate");
            RedisClient targetClient = RedisClient.create(RedisURI.create(TARGET_HOST, TARGET_PORT));
            Long targetSize = targetClient.connect().sync().dbsize();
            Assertions.assertEquals(sourceSize, targetSize);
        } finally {
            target.stop();
        }
    }

    @Test
    public void testReplicateLive() throws Exception {
        RedisServer target = serverBuilder(TARGET_PORT).build();
        try {
            target.start();
            runFile("gen-faker");
            Thread replicateThread = new Thread(() -> runFile("replicate-live"));
            replicateThread.start();
            Thread.sleep(500);
            RedisCommands<String, String> commands = commands();
            int count = 39;
            for (int index = 0; index < count; index++) {
                commands.set("string:" + index, "value" + index);
                Thread.sleep(1);
            }
            Thread.sleep(1000);
            log.info("Interrupting");
            replicateThread.interrupt();
            RedisClient targetClient = RedisClient.create(RedisURI.create(TARGET_HOST, TARGET_PORT));
            Long sourceSize = commands.dbsize();
            Assertions.assertTrue(sourceSize > 0);
            Long targetSize = targetClient.connect().sync().dbsize();
            Assertions.assertEquals(sourceSize, targetSize);
        } finally {
            target.stop();
        }
    }
}
