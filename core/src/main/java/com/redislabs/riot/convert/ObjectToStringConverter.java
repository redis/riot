package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

public class ObjectToStringConverter implements Converter<Object, String> {

	@Override
	public String convert(Object source) {
		return source.toString();
	}

}
