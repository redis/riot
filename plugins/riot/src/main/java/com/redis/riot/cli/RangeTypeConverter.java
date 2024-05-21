package com.redis.riot.cli;

import org.springframework.util.StringUtils;

import picocli.CommandLine.ITypeConverter;

public class RangeTypeConverter<T> implements ITypeConverter<T> {

	public static final String SEPARATOR = ":";
	private static final String VARIABLE = "*";

	private final Factory<T> factory;

	public RangeTypeConverter(Factory<T> function) {
		this.factory = function;
	}

	@Override
	public T convert(String value) {
		return of(value, SEPARATOR);
	}

	private T of(String string, String separator) {
		try {
			int pos = string.indexOf(separator);
			if (pos == -1) {
				int value = Integer.parseInt(string);
				return factory.create(value, value);
			}
			int min = min(string.substring(0, pos).trim());
			int max = max(string.substring(pos + 1).trim());
			return factory.create(min, max);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid range. Range should be in the form 'int:int'", e);
		}

	}

	private static int min(String string) {
		if (StringUtils.hasLength(string)) {
			return Integer.parseInt(string);
		}
		return 0;
	}

	private static int max(String string) {
		if (StringUtils.hasLength(string) && !string.equals(VARIABLE)) {
			return Integer.parseInt(string);
		}
		return Integer.MAX_VALUE;
	}

	public static interface Factory<T> {

		T create(int start, int end);

	}

}
