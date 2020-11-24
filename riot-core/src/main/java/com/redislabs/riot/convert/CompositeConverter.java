package com.redislabs.riot.convert;

import org.springframework.core.convert.converter.Converter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class CompositeConverter implements Converter {

	private final Converter[] delegates;

	public CompositeConverter(Converter... delegates) {
		this.delegates = delegates;
	}

	@Override
	public Object convert(Object source) {
		Object result = source;
		for (Converter delegate : delegates) {
			result = delegate.convert(result);
		}
		return result;
	}

}
