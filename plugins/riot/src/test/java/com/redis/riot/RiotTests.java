package com.redis.riot;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.simple.SimpleLogger;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import com.redis.lettucemod.RedisModulesUtils;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.Replicate.CompareMode;
import com.redis.riot.core.ProgressStyle;
import com.redis.spring.batch.item.redis.RedisItemReader.ReaderMode;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.Range;
import com.redis.spring.batch.item.redis.gen.GeneratorItemReader;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.ByteArrayCodec;

abstract class RiotTests extends AbstractRiotApplicationTestBase {

	@BeforeAll
	void setDefaults() {
		setIdleTimeout(Duration.ofSeconds(1));
	}

	protected void runLiveReplication(TestInfo info, String filename) throws Exception {
		DataType[] types = new DataType[] { DataType.HASH, DataType.STRING };
		enableKeyspaceNotifications();
		generate(info, generator(3000, types));
		GeneratorItemReader generator = generator(3500, types);
		generator.setCurrentItemCount(3001);
		generateAsync(testInfo(info, "async"), generator);
		execute(info, filename);
		assertCompare(info);
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

	protected void execute(Replicate replicate, TestInfo info) throws Exception {
		System.setProperty(SimpleLogger.LOG_KEY_PREFIX + ReplicateWriteLogger.class.getName(), "error");
		replicate.getJobArgs().getProgressArgs().setStyle(ProgressStyle.NONE);
		replicate.setJobName(name(info));
		replicate.setJobRepository(jobRepository);
		replicate.setSourceRedisUri(redisURI);
		replicate.getSourceRedisArgs().setCluster(getRedisServer().isRedisCluster());
		replicate.setTargetRedisUri(targetRedisURI);
		replicate.getTargetRedisArgs().setCluster(getTargetRedisServer().isRedisCluster());
		replicate.getSourceRedisReaderArgs().setIdleTimeout(DEFAULT_IDLE_TIMEOUT_SECONDS);
		replicate.call();
	}

	@Test
	void replicateBinaryStruct(TestInfo info) throws Exception {
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
		replication.setCompareMode(CompareMode.NONE);
		replication.setStruct(true);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().hget(key, key), targetConnection.sync().hget(key, key));
	}

	@Test
	void replicateBinaryKeyValueScan(TestInfo info) throws Exception {
		byte[] key = Hex.decode("aced0005");
		byte[] value = Hex.decode("aced0004");
		StatefulRedisModulesConnection<byte[], byte[]> connection = RedisModulesUtils.connection(redisClient,
				ByteArrayCodec.INSTANCE);
		StatefulRedisModulesConnection<byte[], byte[]> targetConnection = RedisModulesUtils
				.connection(targetRedisClient, ByteArrayCodec.INSTANCE);
		connection.sync().set(key, value);
		Replicate replication = new Replicate();
		replication.setCompareMode(CompareMode.NONE);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
	}

	@Test
	void replicateBinaryKeyLive(TestInfo info) throws Exception {
		byte[] key = Hex.decode("aced0005");
		byte[] value = Hex.decode("aced0004");
		StatefulRedisModulesConnection<byte[], byte[]> connection = RedisModulesUtils.connection(redisClient,
				ByteArrayCodec.INSTANCE);
		StatefulRedisModulesConnection<byte[], byte[]> targetConnection = RedisModulesUtils
				.connection(targetRedisClient, ByteArrayCodec.INSTANCE);
		enableKeyspaceNotifications();
		Executors.newSingleThreadExecutor().execute(() -> {
			awaitSubscribers();
			connection.sync().set(key, value);
		});
		Replicate replication = new Replicate();
		replication.getSourceRedisReaderArgs().setMode(ReaderMode.LIVE);
		replication.setCompareMode(CompareMode.NONE);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
	}

	@Test
	void filterKeySlot(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		Replicate replication = new Replicate();
		replication.getSourceRedisReaderArgs().setMode(ReaderMode.LIVE);
		replication.setCompareMode(CompareMode.NONE);
		replication.getSourceRedisReaderArgs().getKeyFilterArgs().setSlots(Arrays.asList(new Range(0, 8000)));
		generateAsync(info, generator(100));
		execute(replication, info);
		awaitNoSubscribers();
		Assertions.assertTrue(targetRedisCommands.keys("*").stream().map(SlotHash::getSlot).allMatch(between(0, 8000)));
	}

	private Predicate<Integer> between(int start, int end) {
		return i -> i >= 0 && i <= end;
	}

}
