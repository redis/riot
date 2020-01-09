package com.redislabs.riot;

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
	public void testReplicate() throws Exception {
		RedisExecProvider redisExecProvider = RedisExecProvider.defaultProvider().override(OS.MAC_OS_X,
				"/usr/local/bin/redis-server");
		RedisServer source = RedisServer.builder().port(16379).build();
		try {
			source.start();
			RedisServer target = RedisServer.builder().redisExecProvider(redisExecProvider).port(16380).build();
			try {
				target.start();
				String[] importCommand = CommandLineUtils.translateCommandline(
						"--server localhost:16379 gen --threads 4 -d field1=100 field2=1000 --batch 100 --max 100000 hmset --keyspace test --keys index");
				new Riot().execute(importCommand);
				RedisClient sourceClient = RedisClient.create(RedisURI.create("localhost", 16379));
				new Thread(() -> {
					try {
						Thread.sleep(500);
						RedisCommands<String, String> commands = sourceClient.connect().sync();
						for (int index = 0; index < 5; index++) {
							String key = "key" + index;
							commands.set(key, "value" + index);
							Thread.sleep(100);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}).start();
				String[] replicateCommand = CommandLineUtils.translateCommandline(
						"--server localhost:16380 replicate --threads 5 --server localhost:16379");
				new Riot().execute(replicateCommand);
				RedisClient targetClient = RedisClient.create(RedisURI.create("localhost", 16380));
				Long sourceSize = sourceClient.connect().sync().dbsize();
				Long targetSize = targetClient.connect().sync().dbsize();
				Assertions.assertEquals(sourceSize, targetSize);
			} finally {
				target.stop();
			}
		} finally {
			source.stop();
		}
	}

}
