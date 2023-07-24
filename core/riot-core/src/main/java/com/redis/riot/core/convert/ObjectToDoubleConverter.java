package com.redis.riot.core.convert;

import java.util.function.ToDoubleFunction;

public class ObjectToDoubleConverter implements ToDoubleFunction<Object> {

	private final double defaultValue;

	public ObjectToDoubleConverter(double defaultValue) {
		this.defaultValue = defaultValue;
	}

	@Override
	public double applyAsDouble(Object source) {
		if (source != null) {
			if (source instanceof Number) {
				return ((Number) source).doubleValue();
			}
			if (source instanceof String) {
				String string = (String) source;
				if (!string.isEmpty()) {
					return Double.parseDouble(string);
				}
			}
		}
		return defaultValue;
	}

}
