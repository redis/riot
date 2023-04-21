package com.redis.riot.cli;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class TargetCommandContext extends JobCommandContext {

	private final RedisOptions targetRedisOptions;
	private final AbstractRedisClient targetRedisClient;
	private final RedisURI targetRedisURI;

	public TargetCommandContext(JobRunner jobRunner, RedisOptions redisOptions, RedisOptions targetRedisOptions) {
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

	public <K, V> RedisItemReader.Builder<K, V> targetReader(RedisCodec<K, V> codec) {
		return reader(targetRedisClient, codec);
	}

	public RedisItemReader.Builder<String, String> targetReader() {
		return targetReader(StringCodec.UTF8);
	}

	public RedisItemWriter.Builder<String, String> targetWriter() {
		return targetWriter(StringCodec.UTF8);
	}

	public <K, V> RedisItemWriter.Builder<K, V> targetWriter(RedisCodec<K, V> codec) {
		return writer(targetRedisClient, codec);
	}

}
