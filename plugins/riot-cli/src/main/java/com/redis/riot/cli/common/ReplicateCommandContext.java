package com.redis.riot.cli.common;

import com.redis.spring.batch.RedisItemReader.ComparatorBuilder;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.RedisItemWriter.DataStructureWriterBuilder;
import com.redis.spring.batch.RedisItemWriter.KeyDumpWriterBuilder;
import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.common.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class ReplicateCommandContext extends CommandContext {

	private final RedisOptions targetRedisOptions;
	private final AbstractRedisClient targetRedisClient;
	private final RedisURI targetRedisURI;

	public ReplicateCommandContext(JobRunner jobRunner, RedisOptions redisOptions, RedisOptions targetRedisOptions) {
		super(jobRunner, redisOptions);
		this.targetRedisOptions = targetRedisOptions;
		this.targetRedisURI = targetRedisOptions.uri();
		this.targetRedisClient = targetRedisOptions.client();
	}

	@Override
	public void close() throws Exception {
		targetRedisClient.shutdown();
		targetRedisClient.getResources().shutdown();
		super.close();
	}

	public RedisOptions getTargetRedisOptions() {
		return targetRedisOptions;
	}

	public AbstractRedisClient getTargetRedisClient() {
		return targetRedisClient;
	}

	public RedisURI getTargetRedisURI() {
		return targetRedisURI;
	}

	public ComparatorBuilder comparator() {
		return new ComparatorBuilder(getRedisClient(), targetRedisClient).jobRunner(jobRunner);
	}

	public <K, V> ScanBuilder<K, V, DataStructure<K>> targetDataStructureReader(RedisCodec<K, V> codec) {
		return dataStructureReader(targetRedisClient, codec);
	}

	public ScanBuilder<String, String, DataStructure<String>> targetDataStructureReader() {
		return targetDataStructureReader(StringCodec.UTF8);
	}

	public DataStructureWriterBuilder<String, String> targetDataStructureWriter() {
		return targetDataStructureWriter(StringCodec.UTF8);
	}

	public <K, V> DataStructureWriterBuilder<K, V> targetDataStructureWriter(RedisCodec<K, V> codec) {
		return dataStructureWriter(targetRedisClient, codec);
	}

	public KeyDumpWriterBuilder<byte[], byte[]> targetKeyDumpWriter() {
		return new KeyDumpWriterBuilder<>(targetRedisClient, ByteArrayCodec.INSTANCE);
	}

}
