package com.redis.riot;

import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;

import com.redis.spring.batch.support.JobRunner;

import io.lettuce.core.AbstractRedisClient;

public class JobCommandContext implements AutoCloseable {

	private final JobRunner jobRunner;
	private final RedisOptions redisOptions;
	private final AbstractRedisClient redisClient;

	public JobCommandContext(JobRunner jobRunner, RedisOptions redisOptions) {
		this.jobRunner = jobRunner;
		this.redisOptions = redisOptions;
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

}
