package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.resource.DefaultClientResources;

public class TestLettucePipeline {

	public static void main(String[] args) {
		RedisClient client = RedisClient.create(DefaultClientResources.create(), RedisURI.create("localhost", 6379));
		StatefulRedisConnection<String, String> connection = client.connect();
		List<RedisFuture<?>> futures = new ArrayList<>();
		RedisAsyncCommands<String, String> commands = connection.async();
		commands.setAutoFlushCommands(false);
		for (int index = 0; index < 10; index++) {
			RedisFuture<String> future = commands.set("key" + index, "sdfdsf");
			if (future != null) {
				futures.add(future);
			}
			RedisFuture<Long> incrFuture = commands.incr("key" + index);
			if (incrFuture != null) {
				futures.add(incrFuture);
			}
		}
		commands.flushCommands();
		for (RedisFuture<?> future : futures) {
			try {
				future.get(5, TimeUnit.SECONDS);
			} catch (Exception e) {
				System.err.println("Error: " + e.getMessage());
			}
		}
	}
}
