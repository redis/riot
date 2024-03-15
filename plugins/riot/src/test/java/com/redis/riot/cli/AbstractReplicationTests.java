package com.redis.riot.cli;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;

import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.test.KeyspaceComparison;
import com.redis.spring.batch.writer.StructItemWriter;

import io.lettuce.core.cluster.SlotHash;

abstract class AbstractReplicationTests extends AbstractRiotTestBase {

	private final Log log = LogFactory.getLog(getClass());

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
	void replicateDryRun(TestInfo info) throws Throwable {
		String filename = "replicate-dry-run";
		generate(info, generator(73));
		Assertions.assertTrue(commands.dbsize() > 0);
		execute(info, filename);
		Assertions.assertEquals(0, targetCommands.dbsize());
	}

	@Test
	void replicateHLL(TestInfo info) throws Throwable {
		String key = "crawled:20171124";
		String value = "http://www.google.com/";
		commands.pfadd(key, value);
		Assertions.assertEquals(0, execute(info, "replicate-hll"));
		Assertions.assertTrue(compare(info).isOk());
	}

	@Test
	void replicateKeyProcessor(TestInfo info) throws Throwable {
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
	void replicateKeyExclude(TestInfo info) throws Throwable {
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
	void replicateLiveKeyExclude(TestInfo info) throws Throwable {
		int goodCount = 200;
		int badCount = 100;
		String filename = "replicate-live-key-exclude";
		enableKeyspaceNotifications(client);
		Executors.newSingleThreadScheduledExecutor().execute(() -> {
			awaitPubSub();
			GeneratorItemReader generator = generator(goodCount, DataType.HASH);
			GeneratorItemReader generator2 = generator(badCount, DataType.HASH);
			generator2.setKeyspace("bad");
			try {
				generate(testInfo(info, "1"), generator);
				generate(testInfo(info, "2"), generator2);
			} catch (Exception e) {
				log.error("Could not generate data", e);
			}
		});
		execute(info, filename);
		Assertions.assertEquals(badCount, keyCount("bad:*"));
		Assertions.assertEquals(goodCount, targetCommands.keys("gen:*").size());
	}

	@Test
	void replicateLive(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live");
	}

	@Test
	void replicateLiveMultiThreaded(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-threads");
	}

	@Test
	void replicateLiveStruct(TestInfo info) throws Exception {
		runLiveReplication(info, "replicate-live-struct");
	}

	@Test
	void replicateLiveKeySlot(TestInfo info) throws Exception {
		String filename = "replicate-live-keyslot";
		enableKeyspaceNotifications(client);
		GeneratorItemReader generator = generator(300);
		Executors.newSingleThreadScheduledExecutor().execute(() -> {
			awaitPubSub();
			TestInfo genInfo = testInfo(info, filename, "gen-live");
			try {
				run(genInfo, step(genInfo, 1, generator, null, RedisItemWriter.struct(client)));
			} catch (JobExecutionException e) {
				log.error("Could not generate data", e);
			}
		});
		execute(info, filename);
		List<String> keys = targetCommands.keys("*");
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
		Assertions.assertTrue(commands.dbsize() > 0);
		execute(info, filename);
	}

	protected void runLiveReplication(TestInfo info, String filename) throws Exception {
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
		enableKeyspaceNotifications(client);
		generate(info, generator(3000, types));
		Executors.newSingleThreadScheduledExecutor().execute(() -> {
			awaitPubSub();
			GeneratorItemReader generator = generator(3500, types);
			generator.setCurrentItemCount(3001);
			StructItemWriter<String, String> writer = RedisItemWriter.struct(client);
			TestInfo stepInfo = testInfo(info, filename, "gen-live");
			SimpleStepBuilder<KeyValue<String>, KeyValue<String>> step = step(stepInfo, 1, generator, null, writer);
			try {
				run(testInfo(info, filename, "gen-live"), step);
				log.info("Generated data");
			} catch (Exception e) {
				log.error("Could not generate data", e);
			}
		});
		execute(info, filename);
		KeyspaceComparison comparison = compare(info);
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

}
