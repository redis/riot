package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Setter;

public abstract class AbstractCollectionRedisItemWriter extends AbstractRedisItemWriter {

	@Setter
	private String[] fields;

	protected String member(Map<String, Object> item) {
		return converter.joinFields(item, fields);
	}

}
