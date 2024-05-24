package com.redis.riot.function;

import java.util.function.Function;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

public class ObjectMapperFunction<T> implements Function<T, String> {

	private final ObjectWriter writer;

	public ObjectMapperFunction(ObjectWriter writer) {
		this.writer = writer;
	}

	@Override
	public String apply(T source) {
		try {
			return writer.writeValueAsString(source);
		} catch (JsonProcessingException e) {
			TypeDescriptor sourceDescriptor = TypeDescriptor.forObject(source);
			TypeDescriptor targetDescriptor = TypeDescriptor.valueOf(String.class);
			throw new ConversionFailedException(sourceDescriptor, targetDescriptor, source, e);
		}
	}

}
