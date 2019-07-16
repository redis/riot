package com.redislabs.riot.redis.writer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

public class StringObjectWriter extends AbstractStringWriter {

	private final static Logger log = LoggerFactory.getLogger(StringObjectWriter.class);

	private ObjectWriter objectWriter;

	public StringObjectWriter(ObjectWriter objectWriter) {
		this.objectWriter = objectWriter;
	}

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
