package com.redis.riot.redis;

import com.redislabs.riot.redis.App;
import com.redislabs.riot.test.BaseTest;
import com.redislabs.riot.test.DataPopulator;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.embedded.RedisServer;

public class TestReplicate extends BaseTest {

    private final static Logger log = LoggerFactory.getLogger(TestReplicate.class);

    private final static String TARGET_HOST = "localhost";
    private final static int TARGET_PORT = 6380;

    @Override
    protected int execute(String[] args) {
        return new App().execute(args);
    }

    @Override
    protected String applicationName() {
        return "riot-redis";
    }

    @Test
    public void testReplicate() {
        RedisServer target = serverBuilder(TARGET_PORT).build();
        try {
            target.start();
            DataPopulator.builder().connection(connection).build().run();
            Long sourceSize = commands().dbsize();
            Assertions.assertTrue(sourceSize > 0);
            runFile("/replicate.txt");
            RedisClient targetClient = RedisClient.create(RedisURI.create(TARGET_HOST, TARGET_PORT));
            Assertions.assertEquals(sourceSize, targetClient.connect().sync().dbsize());
        } finally {
            target.stop();
        }
    }

    @Test
    public void testReplicateLive() throws Exception {
        RedisServer target = serverBuilder(TARGET_PORT).build();
        try {
            target.start();
            DataPopulator.builder().connection(connection).build().run();
            Thread replicateThread = new Thread(() -> runFile("/replicate-live.txt"));
            replicateThread.start();
            Thread.sleep(500);
            RedisCommands<String, String> commands = commands();
            int count = 39;
            for (int index = 0; index < count; index++) {
                commands.set("livestring:" + index, "value" + index);
                Thread.sleep(1);
            }
            Thread.sleep(300);
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
