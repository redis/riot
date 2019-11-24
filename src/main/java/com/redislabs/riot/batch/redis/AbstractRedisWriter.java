package com.redislabs.riot.batch.redis;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public abstract class AbstractRedisWriter<R, O> implements RedisWriter<R, O> {

	private final ConversionService conversionService = new DefaultConversionService();

	@Setter
	private RedisCommands<R> commands;

	protected <T> T convert(Object source, Class<T> targetType) {
		return conversionService.convert(source, targetType);
	}

	@Override
	public Object write(R redis, O item) throws Exception {
		return write(commands, redis, item);
	}

	protected abstract Object write(RedisCommands<R> commands, R redis, O item) throws Exception;

}
