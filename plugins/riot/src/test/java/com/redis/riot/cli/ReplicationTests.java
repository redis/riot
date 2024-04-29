package com.redis.riot.cli;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.gen.Item.Type;
import com.redis.spring.batch.test.KeyspaceComparison;
import com.redis.spring.batch.util.Await;

import io.lettuce.core.cluster.SlotHash;

abstract class ReplicationTests extends AbstractRiotTestBase {

	@BeforeAll
	void setDefaults() {
		setIdleTimeout(Duration.ofSeconds(1));
	}

	@Test
	void replicate(TestInfo info) throws Throwable {
		String filename = "replicate";
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
	}

	@Test
	void replicateDryRun(TestInfo info) throws Throwable {
		String filename = "replicate-dry-run";
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
		Assertions.assertEquals(0, targetRedisCommands.dbsize());
	}

	@Test
	void replicateHyperloglog(TestInfo info) throws Throwable {
		String key = "crawled:20171124";
		String value = "http://www.google.com/";
		redisCommands.pfadd(key, value);
		Assertions.assertEquals(0, execute(info, "replicate-hll"));
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replicateKeyProcessor(TestInfo info) throws Throwable {
		String filename = "replicate-key-processor";
		GeneratorItemReader gen = generator(1, Type.HASH);
		generate(info, gen);
		Long sourceSize = redisCommands.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute(info, filename);
		Assertions.assertEquals(sourceSize, targetRedisCommands.dbsize());
		Assertions.assertEquals(redisCommands.hgetall("gen:1"), targetRedisCommands.hgetall("0:gen:1"));
	}

	@Test
	void replicateLiveKeyExclude(TestInfo info) throws Throwable {
		int goodCount = 200;
		int badCount = 100;
		enableKeyspaceNotifications();
		generateAsync(testInfo(info, "gen-1"), generator(goodCount, Type.HASH));
		GeneratorItemReader generator2 = generator(badCount, Type.HASH);
		generator2.setKeyspace("bad");
		generateAsync(testInfo(info, "gen-2"), generator2);
		execute(info, "replicate-live-key-exclude");
		Await.await().until(() -> redisCommands.pubsubNumpat() == 0);
		Assertions.assertEquals(badCount, keyCount("bad:*"));
		Assertions.assertEquals(0, targetRedisCommands.keys("bad:*").size());
		Assertions.assertEquals(goodCount, targetRedisCommands.keys("gen:*").size());
	}

	@Test
	void replicateLive(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live");
	}

	@Test
	void replicateLiveThreads(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-threads");
	}

	@Test
	void replicateLiveStruct(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-struct");
	}

	@Test
	void replicateLiveOnlyStruct(TestInfo info) throws Exception {
		Type[] types = new Type[] { Type.HASH, Type.STRING };
		enableKeyspaceNotifications();
		GeneratorItemReader generator = generator(3500, types);
		generator.setCurrentItemCount(3001);
		generateAsync(testInfo(info, "async"), generator);
		execute(info, "replicate-live-only-struct");
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void replicateLiveKeySlot(TestInfo info) throws Exception {
		String filename = "replicate-live-keyslot";
		enableKeyspaceNotifications();
		GeneratorItemReader generator = generator(300);
		generateAsync(info, generator);
		execute(info, filename);
		List<String> keys = targetRedisCommands.keys("*");
		for (String key : keys) {
			int slot = SlotHash.getSlot(key);
			Assertions.assertTrue(slot >= 0 && slot <= 8000);
		}
	}

	@Test
	void replicateStruct(TestInfo info) throws Throwable {
		String filename = "replicate-struct";
		GeneratorItemReader generator = generator(12000);
		generate(info, generator);
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		execute(info, filename);
	}

	protected void runLiveReplication(TestInfo info, String filename) throws Exception {
		Type[] types = new Type[] { Type.HASH, Type.STRING };
		enableKeyspaceNotifications();
		generate(info, generator(3000, types));
		GeneratorItemReader generator = generator(3500, types);
		generator.setCurrentItemCount(3001);
		generateAsync(testInfo(info, "async"), generator);
		execute(info, filename);
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

}
