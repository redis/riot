package com.redis.riot.core;

import com.redis.spring.batch.RedisItemWriter;

public abstract class AbstractImport extends AbstractRedisCallable {

	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public RedisWriterOptions getWriterOptions() {
		return writerOptions;
	}

	public void setWriterOptions(RedisWriterOptions options) {
		this.writerOptions = options;
	}

	@Override
	protected <K, V, T> void configure(RedisItemWriter<K, V, T> writer) {
		writerOptions.configure(writer);
		super.configure(writer);
	}

}
