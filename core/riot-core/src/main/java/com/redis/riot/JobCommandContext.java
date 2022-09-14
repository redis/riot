package com.redis.riot;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.common.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.pubsub.StatefulRedisPubSubConnection;

public class JobCommandContext implements AutoCloseable {

	private final JobRunner jobRunner;
	private final RedisOptions redisOptions;
	private final AbstractRedisClient redisClient;
	private final RedisURI redisURI;

	public JobCommandContext(JobRunner jobRunner, RedisOptions redisOptions) {
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

	public JobBuilder job(String name) {
		return jobRunner.job(name);
	}

	public StepBuilder step(String name) {
		return jobRunner.step(name);
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

	public GenericObjectPool<StatefulConnection<String, String>> pool() {
		return redisOptions.pool(redisClient);
	}

	public <K, V> GenericObjectPool<StatefulConnection<K, V>> pool(RedisCodec<K, V> codec) {
		return redisOptions.pool(redisClient, codec);
	}

}
