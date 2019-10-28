package com.redislabs.riot.batch.redis.writer;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.riot.batch.redis.RedisConverter;

public abstract class AbstractFlatMapWriter implements RedisMapWriter {

	private ConversionService conversionService = new DefaultConversionService();
	private RedisConverter converter;

	public RedisConverter getConverter() {
		return converter;
	}

	public void setConverter(RedisConverter converter) {
		this.converter = converter;
	}

	protected <T> T convert(Object source, Class<T> targetType) {
		return conversionService.convert(source, targetType);
	}

	protected String join(Map<String, Object> item, String[] fields) {
		return converter.join(item, fields);
	}

	protected String key(Map<String, Object> item) {
		return converter.key(item);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Map stringMap(Map map) {
		map.forEach((k, v) -> map.put(k, conversionService.convert(v, String.class)));
		return map;
	}

}
