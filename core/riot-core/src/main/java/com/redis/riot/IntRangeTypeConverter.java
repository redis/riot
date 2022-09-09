package com.redis.riot;

import com.redis.spring.batch.common.IntRange;

import picocli.CommandLine.ITypeConverter;

public class IntRangeTypeConverter implements ITypeConverter<IntRange> {

	@Override
	public IntRange convert(String value) {
		int separator;
		if ((separator = value.indexOf(IntRange.SEPARATOR)) >= 0) {
			return IntRange.between(parse(value.substring(0, separator), 0),
					parse(value.substring(separator + IntRange.SEPARATOR.length()), Integer.MAX_VALUE));
		}
		return IntRange.is(parse(value, Integer.MAX_VALUE));
	}

	private int parse(String string, int defaultValue) {
		if (string.isEmpty()) {
			return defaultValue;
		}
		return Integer.parseInt(string);
	}

}
