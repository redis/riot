package com.redis.riot.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.ScanSizeEstimator;
import com.redis.riot.core.SlotRange;
import com.redis.riot.redis.Replication.LoggingWriteListener;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.gen.GeneratorItemReader;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.spring.batch.test.KeyspaceComparison;
import com.redis.testcontainers.RedisServer;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;

public abstract class ReplicationTests extends AbstractTargetTestBase {

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

	protected void execute(Replication replication, TestInfo info) throws Exception {
		try (replication) {
			System.setProperty(SimpleLogger.LOG_KEY_PREFIX + LoggingWriteListener.class.getName(), "error");
			replication.setName(name(info));
			replication.setJobFactory(jobFactory);
			replication.setRedisURI(RedisURI.create(getRedisServer().getRedisURI()));
			replication.setRedisClientOptions(redisOptions(getRedisServer()));
			replication.setTargetRedisURI(RedisURI.create(getTargetRedisServer().getRedisURI()));
			replication.setTargetRedisClientOptions(redisOptions(getTargetRedisServer()));
			replication.setIdleTimeout(getIdleTimeout());
			replication.afterPropertiesSet();
			replication.call();
		}
	}

	private RedisClientOptions redisOptions(RedisServer redis) {
		RedisClientOptions options = new RedisClientOptions();
		options.setCluster(redis.isRedisCluster());
		return options;
	}

	@Test
	void replication(TestInfo info) throws Throwable {
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		Replication replication = new Replication();
		execute(replication, info);
		KeyspaceComparison<String> comparison = compare(info);
		Assertions.assertFalse(comparison.getAll().isEmpty());
		Assertions.assertEquals(Collections.emptyList(), comparison.mismatches());
	}

	@Test
	void keyProcessor(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replication replication = new Replication();
		replication.setType(ReplicationType.STRUCT);
		replication.setProcessorOptions(processorOptions("#{type}:#{key}"));
		execute(replication, info);
		Assertions.assertEquals(value1, targetRedisCommands.get("string:" + key1));
	}

	private KeyValueProcessorOptions processorOptions(String keyExpression) {
		KeyValueProcessorOptions options = new KeyValueProcessorOptions();
		options.setKeyExpression(RiotUtils.parseTemplate(keyExpression));
		return options;
	}

	@Test
	void keyProcessorWithDate(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replication replication = new Replication();
		replication.setProcessorOptions(processorOptions(
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
		Replication replication = new Replication();
		replication.setCompareMode(CompareMode.NONE);
		replication.setType(ReplicationType.STRUCT);
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
		Replication replication = new Replication();
		replication.setCompareMode(CompareMode.NONE);
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
		Replication replication = new Replication();
		replication.setMode(ReplicationMode.LIVE);
		replication.setCompareMode(CompareMode.NONE);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
	}

	@Test
	void estimateScanSize(TestInfo info) throws Exception {
		GeneratorItemReader gen = generator(3000, DataType.HASH, DataType.STRING);
		generate(info, gen);
		long expectedCount = redisCommands.dbsize();
		ScanSizeEstimator estimator = new ScanSizeEstimator(redisClient);
		estimator.setKeyPattern(GeneratorItemReader.DEFAULT_KEYSPACE + ":*");
		estimator.setSamples(300);
		assertEquals(expectedCount, estimator.getAsLong(), expectedCount / 10);
		estimator.setKeyType(DataType.HASH.getString());
		assertEquals(expectedCount / 2, estimator.getAsLong(), expectedCount / 10);
	}

	@Test
	void filterKeySlot(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		Replication replication = new Replication();
		replication.setMode(ReplicationMode.LIVE);
		replication.setCompareMode(CompareMode.NONE);
		replication.getReaderOptions().getKeyFilterOptions().setSlots(Arrays.asList(new SlotRange(0, 8000)));
		generateAsync(info, generator(100));
		execute(replication, info);
		awaitUntilNoSubscribers();
		Assertions.assertTrue(targetRedisCommands.keys("*").stream().map(SlotHash::getSlot).allMatch(between(0, 8000)));
	}

	private Predicate<Integer> between(int start, int end) {
		return i -> i >= 0 && i <= end;
	}

}
