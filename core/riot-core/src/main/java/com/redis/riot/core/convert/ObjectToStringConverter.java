package com.redis.riot.core.convert;

import org.springframework.core.convert.converter.Converter;

public class ObjectToStringConverter implements Converter<Object, String> {

	@Override
	public String convert(Object source) {
		if (source == null) {
			return null;
		}
		return source.toString();
	}

}
