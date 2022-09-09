package com.redis.riot.redis;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.redis.lettucemod.util.RedisClientBuilder;
import com.redis.riot.JobCommandContext;
import com.redis.riot.RedisOptions;
import com.redis.spring.batch.common.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;

public class TargetCommandContext extends JobCommandContext {

	private final RedisOptions targetRedisOptions;
	private final AbstractRedisClient targetRedisClient;
	private final RedisURI targetRedisURI;

	public TargetCommandContext(JobRunner jobRunner, RedisOptions redisOptions, RedisOptions targetRedisOptions) {
		super(jobRunner, redisOptions);
		this.targetRedisOptions = targetRedisOptions;
		RedisClientBuilder clientBuilder = RedisClientBuilder.create(targetRedisOptions.redisClientOptions());
		this.targetRedisURI = clientBuilder.uri();
		this.targetRedisClient = clientBuilder.client();
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

	public GenericObjectPool<StatefulConnection<String, String>> targetPool() {
		return targetPool(StringCodec.UTF8);
	}

	public <K, V> GenericObjectPool<StatefulConnection<K, V>> targetPool(RedisCodec<K, V> codec) {
		return pool(targetRedisClient, codec, targetRedisOptions);
	}

}
