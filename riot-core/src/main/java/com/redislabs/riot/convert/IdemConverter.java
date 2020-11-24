package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class IdemConverter<T> implements Converter<T, T> {

	@Override
	public T convert(T source) {
		return source;
	}

}
