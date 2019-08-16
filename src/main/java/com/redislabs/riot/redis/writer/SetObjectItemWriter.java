package com.redislabs.riot.redis.writer;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

public class SetObjectItemWriter extends SetItemWriter {
	
	private final Logger log = LoggerFactory.getLogger(SetObjectItemWriter.class);

	private ObjectWriter objectWriter;

	public void setObjectWriter(ObjectWriter objectWriter) {
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
