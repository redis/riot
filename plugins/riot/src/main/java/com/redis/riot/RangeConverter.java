package com.redis.riot;

import org.springframework.util.StringUtils;

import com.redis.spring.batch.item.redis.common.Range;

import picocli.CommandLine.ITypeConverter;

public class RangeConverter implements ITypeConverter<Range> {

	public static final String SEPARATOR = "-";

	@Override
	public Range convert(String value) {
		int pos = value.indexOf(SEPARATOR);
		if (pos == -1) {
			int intValue = Integer.parseInt(value);
			return new Range(intValue, intValue);
		}
		int min = Integer.parseInt(value.substring(0, pos).trim());
		int max = max(value.substring(pos + 1).trim());
		return new Range(min, max);
	}

	private static int max(String value) {
		if (StringUtils.hasLength(value)) {
			return Integer.parseInt(value);
		}
		return Integer.MAX_VALUE;
	}

}
