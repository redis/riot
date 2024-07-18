package com.redis.riot.core.processor;

import java.util.function.Function;

public class ObjectToStringFunction implements Function<Object, String> {

	@Override
	public String apply(Object source) {
		if (source == null) {
			return null;
		}
		return source.toString();
	}

}
