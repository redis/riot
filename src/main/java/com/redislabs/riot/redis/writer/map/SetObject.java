package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SetObject<R> extends Set<R> {

	@Setter
	private ObjectWriter objectWriter;

	@Override
	protected String value(Map<String, Object> item) {
		try {
			return objectWriter.writeValueAsString(item);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value: {}", item, e);
			return null;
		}
	}

}
