package com.redis.riot;

import org.springframework.util.Assert;

import com.redis.riot.core.JobExecutionContext;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

public class RedisExecutionContext extends JobExecutionContext {

	private RedisContext redisContext;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(redisContext, "Redis context must not be null");
		redisContext.afterPropertiesSet();
		super.afterPropertiesSet();
	}

	@Override
	public void close() throws Exception {
		if (redisContext != null) {
			redisContext.close();
			redisContext = null;
		}
		super.close();
	}

	public void configure(RedisItemWriter<?, ?, ?> writer) {
		redisContext.configure(writer);
	}

	public void configure(RedisItemReader<?, ?, ?> reader) {
		super.configure(reader);
		redisContext.configure(reader);
	}

	public RedisContext getRedisContext() {
		return redisContext;
	}

	public void setRedisContext(RedisContext context) {
		this.redisContext = context;
	}

}
