package com.redis.riot.core;

import org.springframework.expression.spel.support.StandardEvaluationContext;

public class RiotContext implements AutoCloseable {

	private final RedisContext redisContext;

	private final StandardEvaluationContext evaluationContext;

	public RiotContext(RedisContext redisContext, StandardEvaluationContext evaluationContext) {
		this.redisContext = redisContext;
		this.evaluationContext = evaluationContext;
	}

	public RedisContext getRedisContext() {
		return redisContext;
	}

	public StandardEvaluationContext getEvaluationContext() {
		return evaluationContext;
	}

	@Override
	public void close() {
		redisContext.close();
	}

}
