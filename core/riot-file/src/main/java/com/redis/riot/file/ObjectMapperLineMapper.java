package com.redis.riot.file;

import org.springframework.batch.item.file.LineMapper;

import com.fasterxml.jackson.databind.ObjectMapper;

public class ObjectMapperLineMapper<T> implements LineMapper<T> {

	private final Class<? extends T> valueType;
	private final ObjectMapper mapper;

	public ObjectMapperLineMapper(ObjectMapper mapper, Class<? extends T> valueType) {
		this.mapper = mapper;
		this.valueType = valueType;
	}

	@Override
	public T mapLine(String line, int lineNumber) throws Exception {
		return mapper.readValue(line, valueType);
	}

}