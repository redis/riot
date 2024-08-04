package com.redis.riot;

import org.springframework.util.Assert;

import com.redis.riot.core.JobExecutionContext;
import com.redis.spring.batch.item.redis.RedisItemReader;
import com.redis.spring.batch.item.redis.RedisItemWriter;

public class TargetRedisExecutionContext extends JobExecutionContext {

	private RedisContext sourceRedisContext;
	private RedisContext targetRedisContext;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(sourceRedisContext, "Source Redis context not set");
		Assert.notNull(targetRedisContext, "Target Redis context not set");
		sourceRedisContext.afterPropertiesSet();
		targetRedisContext.afterPropertiesSet();
		super.afterPropertiesSet();
	}

	@Override
	public void close() throws Exception {
		if (targetRedisContext != null) {
			targetRedisContext.close();
			targetRedisContext = null;
		}
		if (sourceRedisContext != null) {
			sourceRedisContext.close();
			sourceRedisContext = null;
		}
		super.close();
	}

	public void configureTargetWriter(RedisItemWriter<?, ?, ?> writer) {
		targetRedisContext.configure(writer);
	}

	public void configureSourceReader(RedisItemReader<?, ?, ?> reader) {
		configure(reader);
		sourceRedisContext.configure(reader);
	}

	public void configureTargetReader(RedisItemReader<?, ?, ?> reader) {
		configure(reader);
		targetRedisContext.configure(reader);
	}

	public RedisContext getSourceRedisContext() {
		return sourceRedisContext;
	}

	public void setSourceRedisContext(RedisContext sourceRedisContext) {
		this.sourceRedisContext = sourceRedisContext;
	}

	public RedisContext getTargetRedisContext() {
		return targetRedisContext;
	}

	public void setTargetRedisContext(RedisContext targetRedisContext) {
		this.targetRedisContext = targetRedisContext;
	}

}
