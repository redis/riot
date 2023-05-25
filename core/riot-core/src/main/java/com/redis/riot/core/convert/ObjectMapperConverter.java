package com.redis.riot.core.convert;

import java.util.function.Function;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

public class ObjectMapperConverter<T> implements Function<T, String> {

	private final ObjectWriter writer;

	public ObjectMapperConverter(ObjectWriter writer) {
		this.writer = writer;
	}

	@Override
	public String apply(T source) {
		try {
			return writer.writeValueAsString(source);
		} catch (JsonProcessingException e) {
			throw new ConversionFailedException(TypeDescriptor.forObject(source), TypeDescriptor.valueOf(String.class),
					source, e);
		}
	}
}
