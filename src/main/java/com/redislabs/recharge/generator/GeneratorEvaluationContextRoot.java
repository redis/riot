package com.redislabs.recharge.generator;

import java.util.Locale;

import org.ruaux.pojofaker.Faker;
import org.springframework.data.redis.connection.StringRedisConnection;

public class GeneratorEvaluationContextRoot extends Faker {

	private RedisSequence redisSequence;
	private StringRedisConnection redis;

	public GeneratorEvaluationContextRoot(StringRedisConnection redis, Locale locale) {
		super(locale);
		this.redis = redis;
		this.redisSequence = new RedisSequence(redis);
	}

	public StringRedisConnection getRedis() {
		return redis;
	}

	public RedisSequence getRedisSequence() {
		return redisSequence;
	}
}
