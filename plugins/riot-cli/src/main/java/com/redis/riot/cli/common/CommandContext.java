package com.redis.riot.cli.common;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter.DataStructureWriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;
import com.redis.spring.batch.common.KeyDump;
import com.redis.spring.batch.common.Operation;
import com.redis.spring.batch.reader.DataStructureCodecReadOperation;
import com.redis.spring.batch.reader.KeyDumpReadOperation;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class CommandContext implements AutoCloseable {

	protected final JobRunner jobRunner;
	private final RedisOptions redisOptions;
	private final AbstractRedisClient redisClient;
	private final RedisURI redisURI;

	public CommandContext(JobRunner jobRunner, RedisOptions redisOptions) {
		this.jobRunner = jobRunner;
		this.redisOptions = redisOptions;
		this.redisURI = redisOptions.uri();
		this.redisClient = redisOptions.client();
	}

	public JobRunner getJobRunner() {
		return jobRunner;
	}

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public AbstractRedisClient getRedisClient() {
		return redisClient;
	}

	public RedisURI getRedisURI() {
		return redisURI;
	}

	@Override
	public void close() throws Exception {
		redisClient.shutdown();
		redisClient.getResources().shutdown();
	}

	public StatefulRedisModulesConnection<String, String> connection() {
		return connection(redisClient);
	}

	protected StatefulRedisModulesConnection<String, String> connection(AbstractRedisClient client) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client).connect();
		}
		return ((RedisModulesClient) client).connect();
	}

	public <K, V> StatefulRedisPubSubConnection<K, V> pubSubConnection(RedisCodec<K, V> codec) {
		return pubSubConnection(redisClient, codec);
	}

	public <K, V> StatefulRedisPubSubConnection<K, V> pubSubConnection(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return ((RedisModulesClusterClient) client).connectPubSub(codec);
		}
		return ((RedisModulesClient) client).connectPubSub(codec);
	}

	public <K, V> ScanBuilder<K, V, DataStructure<K>> dataStructureReader(RedisCodec<K, V> codec) {
		return dataStructureReader(redisClient, codec);
	}

	public ScanBuilder<byte[], byte[], KeyDump<byte[]>> keyDumpReader() {
		return keyDumpReader(redisClient);
	}

	protected ScanBuilder<byte[], byte[], KeyDump<byte[]>> keyDumpReader(AbstractRedisClient client) {
		return scanBuilder(client, ByteArrayCodec.INSTANCE, new KeyDumpReadOperation(client));
	}

	protected <K, V> ScanBuilder<K, V, DataStructure<K>> dataStructureReader(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return scanBuilder(client, codec, new DataStructureCodecReadOperation<>(client, codec));
	}

	private <K, V, T> ScanBuilder<K, V, T> scanBuilder(AbstractRedisClient client, RedisCodec<K, V> codec,
			Operation<K, V, K, T> operation) {
		return new ScanBuilder<>(client, codec, operation).jobRunner(jobRunner);
	}

	public <K, V> DataStructureWriterBuilder<K, V> dataStructureWriter(RedisCodec<K, V> codec) {
		return dataStructureWriter(redisClient, codec);
	}

	protected static <K, V> DataStructureWriterBuilder<K, V> dataStructureWriter(AbstractRedisClient client,
			RedisCodec<K, V> codec) {
		return new DataStructureWriterBuilder<>(client, codec);
	}

}
