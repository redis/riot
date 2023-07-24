package com.redis.riot.core.convert;

import java.util.function.ToLongFunction;

public class ObjectToLongConverter implements ToLongFunction<Object> {

	private final long defaultValue;

	public ObjectToLongConverter(long defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public long applyAsLong(Object source) {
		if (source != null) {
			if (source instanceof Number) {
				return ((Number) source).longValue();
			}
			if (source instanceof String) {
				String string = (String) source;
				if (!string.isEmpty()) {
					return Long.parseLong(string);
				}
			}
		}
		return defaultValue;
	}

}
