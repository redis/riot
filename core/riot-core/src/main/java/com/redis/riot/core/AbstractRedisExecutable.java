package com.redis.riot.core;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.lettucemod.util.RedisModulesUtils;
import com.redis.spring.batch.RedisItemReader;

import io.lettuce.core.AbstractRedisClient;

public abstract class AbstractRedisExecutable extends AbstractJobExecutable {

	private RedisOptions redisOptions = new RedisOptions();
	private AbstractRedisClient redisClient;
	private StatefulRedisModulesConnection<String, String> redisConnection;

	@Override
	public void afterPropertiesSet() throws Exception {
		this.redisClient = redisOptions.client();
		this.redisConnection = RedisModulesUtils.connection(redisClient);
		super.afterPropertiesSet();
	}

	@Override
	public void close() throws Exception {
		super.close();
		if (redisConnection != null) {
			redisConnection.close();
			redisConnection = null;
		}
		if (redisClient != null) {
			redisClient.shutdown();
			redisClient = null;
		}
	}

	public RedisOptions getRedisOptions() {
		return redisOptions;
	}

	public void setRedisOptions(RedisOptions redisOptions) {
		this.redisOptions = redisOptions;
	}

	@Override
	protected void configureRedisReader(RedisItemReader<?, ?, ?> reader) {
		super.configureRedisReader(reader);
		reader.setClient(redisClient);
	}

}
