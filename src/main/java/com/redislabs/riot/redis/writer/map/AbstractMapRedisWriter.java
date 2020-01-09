package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.riot.redis.writer.AbstractRedisWriter;

public abstract class AbstractMapRedisWriter<R> extends AbstractRedisWriter<R, Map<String, Object>> {

	private final ConversionService conversionService = new DefaultConversionService();

	protected <T> T convert(Object source, Class<T> targetType) {
		return conversionService.convert(source, targetType);
	}

}
