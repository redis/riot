package com.redis.riot;

import com.redis.spring.batch.support.JobRunner;

import io.lettuce.core.AbstractRedisClient;

public class JobCommandContext implements AutoCloseable {

	private final String name;
	private final JobRunner jobRunner;
	private final RedisOptions redisOptions;
	private final AbstractRedisClient redisClient;

	public JobCommandContext(String name, JobRunner jobRunner, RedisOptions redisOptions) {
		this.name = name;
		this.jobRunner = jobRunner;
		this.redisOptions = redisOptions;
		this.redisClient = redisOptions.client();
	}

	public String getName() {
		return name;
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

	@Override
	public void close() throws Exception {
		redisClient.shutdown();
		redisClient.getResources().shutdown();
	}

}
