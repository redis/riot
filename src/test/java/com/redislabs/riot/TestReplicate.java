package com.redislabs.riot;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import redis.embedded.RedisServer;

public class TestReplicate extends BaseTest {

	private final static String TARGET_HOST = "localhost";
	private final static int TARGET_PORT = 16380;

	@Test
	public void testReplicate() throws Exception {
		RedisServer target = serverBuilder(TARGET_PORT).build();
		try {
			target.start();
			runCommandWithServer("gen -d field1=100 field2=1000 --max 1000 --keyspace test --keys index");
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
			runCommandWithServer("gen -d field1=100 field2=1000 --max 1000 --keyspace test --keys index");
			ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
			final AtomicInteger index = new AtomicInteger();
			scheduler.scheduleWithFixedDelay(() -> {
				String key = "notificationkey" + index.getAndIncrement();
				commands().set(key, "value");
			}, 1000, 100, TimeUnit.MILLISECONDS);
			Thread replicateThread = new Thread(() -> {
				try {
					runFile("replicate-live");
				} catch (Exception e) {
					e.printStackTrace();
				}
			});
			replicateThread.start();
			Thread.sleep(5000);
			scheduler.shutdown();
			Thread.sleep(500);
			System.out.println("Made " + index.get() + " updates");
			replicateThread.interrupt();
			Thread.sleep(3000);
			RedisClient targetClient = RedisClient.create(RedisURI.create("localhost", 16380));
			Long sourceSize = commands().dbsize();
			Assertions.assertTrue(sourceSize > 0);
			Long targetSize = targetClient.connect().sync().dbsize();
			Assertions.assertEquals(sourceSize, targetSize);
		} finally {
			target.stop();
		}
	}

}
