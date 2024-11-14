package com.redis.riot.file;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.transform.LineAggregator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonLineAggregator<T> implements LineAggregator<T> {

	private final Log log = LogFactory.getLog(getClass());

	private final ObjectMapper mapper;

	public JsonLineAggregator(ObjectMapper mapper) {
		this.mapper = mapper;
	}

	@Override
	public String aggregate(T item) {
		try {
			return mapper.writeValueAsString(item);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize item", e);
			return null;
		}
	}
}