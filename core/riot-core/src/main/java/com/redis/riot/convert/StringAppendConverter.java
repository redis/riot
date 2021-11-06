package com.redis.riot.convert;

import java.util.Collection;

import org.springframework.core.convert.converter.Converter;

public class StringAppendConverter<T> implements Converter<T, String> {

	private final Collection<Converter<T, String>> converters;

	public StringAppendConverter(Collection<Converter<T, String>> converters) {
		this.converters = converters;
	}

	@Override
	public String convert(T source) {
		StringBuilder builder = new StringBuilder();
		for (Converter<T, String> converter : converters) {
			builder.append(converter.convert(source));
		}
		return builder.toString();
	}

}
