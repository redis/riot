package com.redis.riot.core.processor;

import java.util.function.ToLongFunction;

import org.springframework.util.StringUtils;

public class ObjectToLongFunction implements ToLongFunction<Object> {

	@Override
	public long applyAsLong(Object source) {
		if (source != null) {
			if (source instanceof Number) {
				return ((Number) source).longValue();
			}
			if (source instanceof String) {
				String string = (String) source;
				if (StringUtils.hasLength(string)) {
					return Long.parseLong(string);
				}
			}
		}
		return 0;
	}

}