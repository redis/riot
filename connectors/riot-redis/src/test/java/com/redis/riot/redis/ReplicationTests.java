package com.redis.riot.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.springframework.batch.item.support.ListItemWriter;
import org.testcontainers.shaded.org.bouncycastle.util.encoders.Hex;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.ExportProcessorOptions;
import com.redis.riot.core.PredicateItemProcessor;
import com.redis.riot.core.RedisClientOptions;
import com.redis.riot.core.RiotUtils;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.test.AbstractTargetTestBase;
import com.redis.spring.batch.util.Predicates;
import com.redis.testcontainers.RedisServer;

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
		replication.setName(name(info));
		replication.setJobFactory(jobFactory);
		replication.setRedisClientOptions(redisOptions(getRedisServer()));
		replication.setTargetRedisClientOptions(redisOptions(getTargetRedisServer()));
		replication.getReaderOptions().setIdleTimeout(getIdleTimeout());
		replication.execute();
	}

	private RedisClientOptions redisOptions(RedisServer redis) {
		RedisClientOptions options = new RedisClientOptions();
		options.setUri(redis.getRedisURI());
		options.setCluster(redis.isRedisCluster());
		return options;
	}

	@Test
	void replication(TestInfo info) throws Throwable {
		generate(info, generator(73));
		Assertions.assertTrue(redisCommands.dbsize() > 0);
		Replication replication = new Replication();
		execute(replication, info);
		Assertions.assertTrue(compare(info).isOk());
	}

	@Test
	void keyProcessor(TestInfo info) throws Throwable {
		String key1 = "key1";
		String value1 = "value1";
		redisCommands.set(key1, value1);
		Replication replication = new Replication();
		replication.setType(ReplicationType.STRUCT);
		replication.setProcessorOptions(processorOptions("#{type.getCode()}:#{key}"));
		execute(replication, info);
		Assertions.assertEquals(value1, targetRedisCommands.get("string:" + key1));
	}

	private ExportProcessorOptions processorOptions(String keyExpression) {
		ExportProcessorOptions options = new ExportProcessorOptions();
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
			awaitPubSub();
			connection.sync().set(key, value);
		});
		Replication replication = new Replication();
		replication.setMode(ReplicationMode.LIVE);
		replication.setCompareMode(CompareMode.NONE);
		execute(replication, info);
		Assertions.assertArrayEquals(connection.sync().get(key), targetConnection.sync().get(key));
	}

	@Test
	void filterKeySlot(TestInfo info) throws Exception {
		enableKeyspaceNotifications();
		RedisItemReader<String, String, KeyValue<String, Object>> reader = structReader(info);
		live(reader);
		reader.setKeyProcessor(new PredicateItemProcessor<>(Predicates.slotRange(0, 8000)));
		ListItemWriter<KeyValue<String, Object>> writer = new ListItemWriter<>();
		generateAsync(info, generator(100));
		FlushingStepBuilder<KeyValue<String, Object>, KeyValue<String, Object>> step = flushingStep(info, reader,
				writer);
		run(job(info).start(step.build()).build());
		Assertions.assertTrue(writer.getWrittenItems().stream().map(KeyValue::getKey).map(SlotHash::getSlot)
				.allMatch(between(0, 8000)));
	}

	private Predicate<Integer> between(int start, int end) {
		return i -> i >= 0 && i <= end;
	}

}
