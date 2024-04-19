package com.redis.riot.core;

import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemWriter;

public abstract class AbstractStructImport extends AbstractRunnable {

	private RedisWriterOptions writerOptions = new RedisWriterOptions();

	public void setWriterOptions(RedisWriterOptions options) {
		this.writerOptions = options;
	}

	protected RedisItemWriter<String, String, KeyValue<String, Object>> writer() {
		RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
		writer.setClient(getRedisClient());
		writer(writer, writerOptions);
		return writer;
	}

}
