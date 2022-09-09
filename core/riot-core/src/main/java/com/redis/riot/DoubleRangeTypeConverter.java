package com.redis.riot;

import com.redis.spring.batch.common.DoubleRange;

import picocli.CommandLine.ITypeConverter;

public class DoubleRangeTypeConverter implements ITypeConverter<DoubleRange> {

	@Override
	public DoubleRange convert(String value) {
		int pos = value.indexOf(DoubleRange.SEPARATOR);
		if (pos >= 0) {
			return DoubleRange.between(parse(value.substring(0, pos), 0),
					parse(value.substring(pos + DoubleRange.SEPARATOR.length()), Double.MAX_VALUE));
		}
		return DoubleRange.is(parse(value, Double.MAX_VALUE));
	}

	private double parse(String string, double defaultValue) {
		if (string.isEmpty()) {
			return defaultValue;
		}
		return Double.parseDouble(string);
	}

}
