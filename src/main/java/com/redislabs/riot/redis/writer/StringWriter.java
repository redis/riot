package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

@Slf4j
public class StringWriter extends AbstractRedisDataStructureItemWriter {

	private ObjectWriter objectWriter;

	public StringWriter(ObjectWriter objectWriter) {
		this.objectWriter = objectWriter;
	}

	private String value(Map<String, Object> item) throws JsonProcessingException {
		return objectWriter.writeValueAsString(item);
	}

	@Override
	protected Response<String> write(Pipeline pipeline, String key, Map<String, Object> item) {
		try {
			return pipeline.set(key, value(item));
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value: {}", item, e);
			return null;
		}
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		try {
			return commands.set(key, value(item));
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value: {}", item, e);
			return null;
		}
	}

}
