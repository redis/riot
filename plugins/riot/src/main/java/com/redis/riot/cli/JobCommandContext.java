package com.redis.riot.cli;

import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;

import com.redis.lettucemod.RedisModulesClient;
import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.cluster.RedisModulesClusterClient;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.JobRunner;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
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

	public <K, V> RedisItemReader.Builder<K, V> reader(RedisCodec<K, V> codec) {
		return reader(redisClient, codec);
	}

	public RedisItemReader.Builder<String, String> reader() {
		return reader(StringCodec.UTF8);
	}

	protected <K, V> RedisItemReader.Builder<K, V> reader(AbstractRedisClient client, RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemReader.client((RedisModulesClusterClient) client, codec).jobRunner(jobRunner);
		}
		return RedisItemReader.client((RedisModulesClient) client, codec).jobRunner(jobRunner);
	}

	public RedisItemWriter.Builder<String, String> writer() {
		return writer(StringCodec.UTF8);
	}

	public <K, V> RedisItemWriter.Builder<K, V> writer(RedisCodec<K, V> codec) {
		return writer(redisClient, codec);
	}

	protected static <K, V> RedisItemWriter.Builder<K, V> writer(AbstractRedisClient client, RedisCodec<K, V> codec) {
		if (client instanceof RedisModulesClusterClient) {
			return RedisItemWriter.client((RedisModulesClusterClient) client, codec);
		}
		return RedisItemWriter.client((RedisModulesClient) client, codec);
	}

	public StepBuilder step(String name) {
		return jobRunner.step(name);
	}

	public JobBuilder job(String name) {
		return jobRunner.job(name);
	}

}
