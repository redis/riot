package com.redis.riot.redis;

import com.redis.riot.JobCommandContext;
import com.redis.riot.RedisOptions;
import com.redis.spring.batch.support.JobRunner;

import io.lettuce.core.AbstractRedisClient;

public class TargetCommandContext extends JobCommandContext {

	private final RedisOptions targetRedisOptions;
	private final AbstractRedisClient targetRedisClient;

	public TargetCommandContext(JobCommandContext context, RedisOptions targetRedisOptions) {
		this(context.getJobRunner(), context.getRedisOptions(), targetRedisOptions);
	}

	public TargetCommandContext(JobRunner jobRunner, RedisOptions redisOptions, RedisOptions targetRedisOptions) {
		super(jobRunner, redisOptions);
		this.targetRedisOptions = targetRedisOptions;
		this.targetRedisClient = targetRedisOptions.client();
	}

	public RedisOptions getTargetRedisOptions() {
		return targetRedisOptions;
	}

	public AbstractRedisClient getTargetRedisClient() {
		return targetRedisClient;
	}

}
