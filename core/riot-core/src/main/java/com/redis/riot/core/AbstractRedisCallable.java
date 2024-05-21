package com.redis.riot.core;

import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public abstract class AbstractRedisCallable extends AbstractRiotCallable {

	private static final String CONTEXT_VAR_REDIS = "redis";

	private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();
	private RedisClientOptions redisClientOptions = new RedisClientOptions();

	private RedisURI redisURI;
	private AbstractRedisClient redisClient;
	private StatefulRedisModulesConnection<String, String> redisConnection;
	protected RedisModulesCommands<String, String> redisCommands;
	protected StandardEvaluationContext evaluationContext;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(redisURI, "RedisURI not set");
		redisClient = redisClientOptions.redisClient(redisURI);
		redisConnection = RedisModulesUtils.connection(redisClient);
		redisCommands = redisConnection.sync();
		evaluationContext = createEvaluationContext();
		super.afterPropertiesSet();
	}

	protected StandardEvaluationContext createEvaluationContext() throws NoSuchMethodException, SecurityException {
		StandardEvaluationContext context = evaluationContextOptions.evaluationContext();
		context.setVariable(CONTEXT_VAR_REDIS, redisCommands);
		return context;
	}

	@Override
	public void close() {
		evaluationContext = null;
		redisCommands = null;
		if (redisConnection != null) {
			redisConnection.close();
			redisConnection = null;
		}
		if (redisClient != null) {
			redisClient.close();
			redisClient.getResources().shutdown();
		}
	}

	public EvaluationContextOptions getEvaluationContextOptions() {
		return evaluationContextOptions;
	}

	public void setEvaluationContextOptions(EvaluationContextOptions spelProcessorOptions) {
		this.evaluationContextOptions = spelProcessorOptions;
	}

	protected <K, V, T> void configure(RedisItemReader<K, V, T> reader) {
		reader.setClient(redisClient);
		reader.setDatabase(redisURI.getDatabase());
	}

	protected <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
		writer.setClient(redisClient);
	}

	public RedisURI getRedisURI() {
		return redisURI;
	}

	public void setRedisURI(RedisURI redisURI) {
		this.redisURI = redisURI;
	}

	public RedisClientOptions getRedisClientOptions() {
		return redisClientOptions;
	}

	public void setRedisClientOptions(RedisClientOptions options) {
		this.redisClientOptions = options;
	}

}
