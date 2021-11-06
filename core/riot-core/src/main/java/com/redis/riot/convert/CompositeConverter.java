package com.redis.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class CompositeConverter<S, P, T> implements Converter<S, T> {

	private final Converter<S, P> sourceConverter;
	private final Converter<P, T> targetConverter;

	public CompositeConverter(Converter<S, P> sourceConverter, Converter<P, T> targetConverter) {
		this.sourceConverter = sourceConverter;
		this.targetConverter = targetConverter;
	}

	@Override
	public T convert(S source) {
		if (source == null) {
			return null;
		}
		P intermediary = sourceConverter.convert(source);
		if (intermediary == null) {
			return null;
		}
		return targetConverter.convert(intermediary);
	}

}
