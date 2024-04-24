package com.redis.riot.core;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.api.sync.RedisModulesCommands;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.LongExpressionFunction;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemWriter;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;

public abstract class AbstractRedisCallable extends AbstractRiotCallable {

	private static final String CONTEXT_VAR_REDIS = "redis";

	private RedisClientOptions redisClientOptions = new RedisClientOptions();
	private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();

	protected RedisURI redisURI;
	private AbstractRedisClient redisClient;
	private StatefulRedisModulesConnection<String, String> redisConnection;
	protected RedisModulesCommands<String, String> redisCommands;
	private StandardEvaluationContext evaluationContext;

	@Override
	public void afterPropertiesSet() throws Exception {
		redisURI = redisClientOptions.redisURI();
		redisClient = redisClientOptions.client(redisURI);
		redisConnection = RedisModulesUtils.connection(redisClient);
		redisCommands = redisConnection.sync();
		evaluationContext = evaluationContext();
		super.afterPropertiesSet();
	}

	protected StandardEvaluationContext evaluationContext() {
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

	public RedisClientOptions getRedisClientOptions() {
		return redisClientOptions;
	}

	public void setRedisClientOptions(RedisClientOptions options) {
		this.redisClientOptions = options;
	}

	public StandardEvaluationContext getEvaluationContext() {
		return evaluationContext;
	}

	public void setEvaluationContextOptions(EvaluationContextOptions spelProcessorOptions) {
		this.evaluationContextOptions = spelProcessorOptions;
	}

	protected <K, V, T> void configure(RedisItemReader<K, V, T> reader) {
		reader.setClient(redisClient);
	}

	protected <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
		writer.setClient(redisClient);
	}

	protected <T> ExpressionFunction<T, String> expressionFunction(Expression expression) {
		return new ExpressionFunction<>(evaluationContext, expression, String.class);
	}

	protected <T> LongExpressionFunction<T> longExpressionFunction(Expression expression) {
		return new LongExpressionFunction<>(evaluationContext, expression);
	}

}
