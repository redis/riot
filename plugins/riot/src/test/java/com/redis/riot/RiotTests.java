package com.redis.riot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.ProgressStyle;
import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;
import com.redis.spring.batch.item.redis.gen.GeneratorOptions;
import com.redis.spring.batch.test.KeyspaceComparison;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;

abstract class RiotTests extends AbstractRiotTestBase {

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
		GeneratorItemReader gen = generator(1, DataType.HASH);
		generate(info, gen);
		Long sourceSize = redisCommands.dbsize();
		Assertions.assertTrue(sourceSize > 0);
		execute(info, filename);
		Assertions.assertEquals(sourceSize, targetRedisCommands.dbsize());
		Assertions.assertEquals(redisCommands.hgetall("gen:1"), targetRedisCommands.hgetall("0:gen:1"));
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
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
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

	public static final String BEERS_JSON_URL = "https://storage.googleapis.com/jrx/beers.json";
	public static final int BEER_CSV_COUNT = 2410;
	public static final int BEER_JSON_COUNT = 216;

	protected static String name(Map<String, String> beer) {
		return beer.get("name");
	}

	protected static String style(Map<String, String> beer) {
		return beer.get("style");
	}

	protected static double abv(Map<String, String> beer) {
		return Double.parseDouble(beer.get("abv"));
	}

	protected void execute(Replicate replication, TestInfo info) throws Exception {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + LoggingKeyValueWriteListener.class.getName(), "error");
		replication.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
		replication.getJobArgs().setName(name(info));
		replication.setJobRepository(jobRepository);
		replication.setRedisArgs(redisArgs(getRedisServer()));
		RedisArgs targetRedisArgs = redisArgs(getTargetRedisServer());
		replication.getTargetRedisArgs().setUri(targetRedisArgs.getUri());
		replication.getTargetRedisArgs().setCluster(targetRedisArgs.isCluster());
		replication.getRedisReaderArgs().setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
		replication.call();
	}

	private RedisArgs redisArgs(RedisServer redis) {
		RedisArgs args = new RedisArgs();
		args.setUri(RedisURI.create(redis.getRedisURI()));
		args.setCluster(redis.isRedisCluster());
		return args;
	}

	@Test
	void keyProcessor(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replicate command = new Replicate();
		command.setStruct(true);
		command.getProcessorArgs().setKeyValueProcessorArgs(keyValueProcessorArgs("#{type}:#{key}"));
		execute(command, info);
		Assertions.assertEquals(value1, targetRedisCommands.get("string:" + key1));
	}

	private KeyValueProcessorArgs keyValueProcessorArgs(String keyExpression) {
		KeyValueProcessorArgs options = new KeyValueProcessorArgs();
		options.setKeyExpression(RiotUtils.parseTemplate(keyExpression));
		return options;
	}

	@Test
	void keyProcessorWithDate(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replicate replication = new Replicate();
		replication.getProcessorArgs().setKeyValueProcessorArgs(keyValueProcessorArgs(
				String.format("#{#date.parse('%s').getTime()}:#{key}", "2010-05-10T00:00:00.000+0000")));
		execute(replication, info);
		Assertions.assertEquals(value1, targetRedisCommands.get("1273449600000:" + key1));
	}

	@Test
	void binaryKeyValueSnapshotReplicationType(TestInfo info) throws Exception {
		byte[] key = Hex.decode("aced0005");
		byte[] value = Hex.decode("aced0004");
		Map<byte[], byte[]> hash = new HashMap<>();
		hash.put(key, value);
		StatefulRedisModulesConnection<byte[], byte[]> connection = RedisModulesUtils.connection(redisClient,
				ByteArrayCodec.INSTANCE);
		StatefulRedisModulesConnection<byte[], byte[]> targetConnection = RedisModulesUtils
				.connection(targetRedisClient, ByteArrayCodec.INSTANCE);
		connection.sync().hset(key, hash);
		Replicate replication = new Replicate();
		replication.getCompareArgs().setMode(CompareMode.NONE);
		replication.setStruct(true);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().hget(key, key), targetConnection.sync().hget(key, key));
	}

	@Test
	void binaryKeyValueSnapshotReplication(TestInfo info) throws Exception {
		byte[] key = Hex.decode("aced0005");
		byte[] value = Hex.decode("aced0004");
		StatefulRedisModulesConnection<byte[], byte[]> connection = RedisModulesUtils.connection(redisClient,
				ByteArrayCodec.INSTANCE);
		StatefulRedisModulesConnection<byte[], byte[]> targetConnection = RedisModulesUtils
				.connection(targetRedisClient, ByteArrayCodec.INSTANCE);
		connection.sync().set(key, value);
		Replicate replication = new Replicate();
		replication.getCompareArgs().setMode(CompareMode.NONE);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
	}

	@Test
	void binaryKeyLiveReplication(TestInfo info) throws Exception {
		byte[] key = Hex.decode("aced0005");
		byte[] value = Hex.decode("aced0004");
		StatefulRedisModulesConnection<byte[], byte[]> connection = RedisModulesUtils.connection(redisClient,
				ByteArrayCodec.INSTANCE);
		StatefulRedisModulesConnection<byte[], byte[]> targetConnection = RedisModulesUtils
				.connection(targetRedisClient, ByteArrayCodec.INSTANCE);
		enableKeyspaceNotifications();
		Executors.newSingleThreadExecutor().execute(() -> {
			awaitUntilSubscribers();
			connection.sync().set(key, value);
		});
		Replicate replication = new Replicate();
		replication.getRedisReaderArgs().setMode(ReaderMode.LIVE);
		replication.getCompareArgs().setMode(CompareMode.NONE);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
	}

	@Test
	void estimateScanSize(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(3000, DataType.HASH, DataType.STRING);
		generate(info, gen);
		long expectedCount = redisCommands.dbsize();
		RedisScanSizeEstimator estimator = new RedisScanSizeEstimator(redisClient);
		estimator.setKeyPattern(GeneratorOptions.DEFAULT_KEYSPACE + ":*");
		estimator.setSamples(300);
		assertEquals(expectedCount, estimator.getAsLong(), expectedCount / 10);
		estimator.setKeyType(DataType.HASH.getString());
		assertEquals(expectedCount / 2, estimator.getAsLong(), expectedCount / 10);
	}

	@Test
	void filterKeySlot(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		Replicate replication = new Replicate();
		replication.getRedisReaderArgs().setMode(ReaderMode.LIVE);
		replication.getCompareArgs().setMode(CompareMode.NONE);
		replication.getRedisReaderArgs().getKeyFilterArgs().setSlots(Arrays.asList(Range.of(0, 8000)));
		generateAsync(info, generator(100));
		execute(replication, info);
		awaitUntilNoSubscribers();
		Assertions.assertTrue(targetRedisCommands.keys("*").stream().map(SlotHash::getSlot).allMatch(between(0, 8000)));
	}

	private Predicate<Integer> between(int start, int end) {
		return i -> i >= 0 && i <= end;
	}

}
