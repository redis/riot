package com.redislabs.riot;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.plexus.util.cli.CommandLineUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.RedisCommands;
import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.OS;

public class TestReplicate {

	@Test
	public void testStandaloneSource() throws Exception {
		RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider().override(OS.MAC_OS_X,
				"/usr/local/bin/redis-server");
		RedisServer source = RedisServer.builder().setting("notify-keyspace-events AK").port(16379).build();
		try {
			source.start();
			RedisServer target = RedisServer.builder().redisExecProvider(redisExecProvider).port(16380).build();
			try {
				target.start();
				String[] importCommand = CommandLineUtils.translateCommandline(
						"--servers localhost:16379 gen --threads 1 -d field1=100 field2=1000 --max 1000 hmset --keyspace test --keys index");
				new Riot().execute(importCommand);
				RedisClient sourceClient = RedisClient.create(RedisURI.create("localhost", 16379));
				ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
				RedisCommands<String, String> commands = sourceClient.connect().sync();
				final AtomicInteger index = new AtomicInteger();
				scheduler.scheduleWithFixedDelay(() -> {
					String key = "notificationkey" + index.getAndIncrement();
					commands.set(key, "value");
				}, 1000, 100, TimeUnit.MILLISECONDS);
				String[] replicate = CommandLineUtils.translateCommandline(
						"--debug --servers localhost:16380 replicate --flush-rate 50 --threads 1 --servers localhost:16379");
				Thread replicateThread = new Thread(() -> new Riot().execute(replicate));
				replicateThread.start();
				Thread.sleep(5000);
				scheduler.shutdown();
				Thread.sleep(500);
				replicateThread.interrupt();
				Thread.sleep(3000);
				RedisClient targetClient = RedisClient.create(RedisURI.create("localhost", 16380));
				Long sourceSize = sourceClient.connect().sync().dbsize();
				Long targetSize = targetClient.connect().sync().dbsize();
				Assertions.assertEquals(sourceSize, targetSize);
			} finally {
				target.stop();
			}
		} finally

		{
			source.stop();
		}
	}

}
