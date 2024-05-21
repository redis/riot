package com.redis.riot.file;

import org.springframework.batch.item.file.LineMapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.reader.MemKeyValue;

public class MemKeyValueJsonLineMapper implements LineMapper<MemKeyValue<String, Object>> {

	private final ObjectMapper mapper;

	public MemKeyValueJsonLineMapper(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	/**
	 * Interpret the line as a Json object and create a MemKeyValue from it.
	 *
	 * @see LineMapper#mapLine(String, int)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public MemKeyValue<String, Object> mapLine(String line, int lineNumber) throws Exception {
		return mapper.readValue(line, MemKeyValue.class);
	}

}