package com.redis.riot.cli;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.test.KeyspaceComparison;

import io.lettuce.core.cluster.SlotHash;

abstract class AbstractReplicationTests extends AbstractRiotTestBase {

	private final Logger log = LoggerFactory.getLogger(AbstractReplicationTests.class);

	@BeforeAll
	void setDefaults() {
		setIdleTimeout(Duration.ofSeconds(1));
	}

	@Test
	void replicate(TestInfo info) throws Throwable {
		String filename = "replicate";
		generate(info, generator(73));
		Assertions.assertTrue(commands.dbsize() > 0);
		execute(info, filename);
	}

	@Test
	void dryRun(TestInfo info) throws Throwable {
		String filename = "replicate-dry-run";
		generate(info, generator(73));
		Assertions.assertTrue(commands.dbsize() > 0);
		execute(info, filename);
		Assertions.assertEquals(0, targetCommands.dbsize());
	}

	@Test
	void hll(TestInfo info) throws Throwable {
		String key = "crawled:20171124";
		String value = "http://www.google.com/";
		commands.pfadd(key, value);
		Assertions.assertEquals(0, execute(info, "replicate-hll"));
		Assertions.assertTrue(compare(info).isOk());
	}

	@Test
	void keyProcessor(TestInfo info) throws Throwable {
		String filename = "replicate-key-processor";
		GeneratorItemReader gen = generator(1, DataType.HASH);
		generate(info, gen);
		Long sourceSize = commands.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute(info, filename);
		Assertions.assertEquals(sourceSize, targetCommands.dbsize());
		Assertions.assertEquals(commands.hgetall("gen:1"), targetCommands.hgetall("0:gen:1"));
	}

	@Test
	void keyExclude(TestInfo info) throws Throwable {
		String filename = "replicate-key-exclude";
		int goodCount = 200;
		GeneratorItemReader gen = generator(goodCount, DataType.HASH);
		generate(info, gen);
		int badCount = 100;
		GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
		generator2.setKeyspace("bad");
		generate(testInfo(info, "2"), generator2);
		Assertions.assertEquals(badCount, keyCount("bad:*"));
		execute(info, filename);
		Assertions.assertEquals(goodCount, targetCommands.keys("gen:*").size());
	}

	@Test
	void liveKeyExclude(TestInfo info) throws Throwable {
		int goodCount = 200;
		int badCount = 100;
		enableKeyspaceNotifications(client);
		generateAsync(testInfo(info, "gen-1"), generator(goodCount, DataType.HASH));
		GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
		generator2.setKeyspace("bad");
		generateAsync(testInfo(info, "gen-2"), generator2);
		execute(info, "replicate-live-key-exclude");
		Assertions.assertEquals(badCount, keyCount("bad:*"));
		Assertions.assertEquals(0, targetCommands.keys("bad:*").size());
		Assertions.assertEquals(goodCount, targetCommands.keys("gen:*").size());
	}

	@Test
	void live(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live");
	}

	@Test
	void threads(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-threads");
	}

	@Test
	void liveStruct(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-struct");
	}

	@Test
	void liveKeySlot(TestInfo info) throws Exception {
		String filename = "replicate-live-keyslot";
		enableKeyspaceNotifications(client);
		GeneratorItemReader generator = generator(300);
		generateAsync(info, generator);
		log.info("Executing");
		execute(info, filename);
		log.info("Executed");
		List<String> keys = targetCommands.keys("*");
		for (String key : keys) {
			int slot = SlotHash.getSlot(key);
			Assertions.assertTrue(slot >= 0 && slot <= 8000);
		}
	}

	@Test
	void struct(TestInfo info) throws Throwable {
		String filename = "replicate-struct";
		GeneratorItemReader generator = generator(12000);
		generate(info, generator);
		Assertions.assertTrue(commands.dbsize() > 0);
		execute(info, filename);
	}

	protected void runLiveReplication(TestInfo info, String filename) throws Exception {
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
		enableKeyspaceNotifications(client);
		generate(info, generator(3000, types));
		GeneratorItemReader generator = generator(3500, types);
		generator.setCurrentItemCount(3001);
		generateAsync(testInfo(info, "async"), generator);
		execute(info, filename);
		KeyspaceComparison comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

}
