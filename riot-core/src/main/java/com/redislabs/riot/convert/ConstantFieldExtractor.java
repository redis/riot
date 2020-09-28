package com.redislabs.riot.convert;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

public class ConstantFieldExtractor implements Converter<Map<String, Object>, Object> {

	private final Object constant;

	public ConstantFieldExtractor(Object constant) {
		this.constant = constant;
	}

	@Override
	public Object convert(Map<String, Object> source) {
		return constant;
	}
}
