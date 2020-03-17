package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({ "rawtypes", "unchecked" })
public abstract class Set extends AbstractKeyMapCommandWriter {

	public static enum Format {
		RAW, XML, JSON
	}

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, Map<String, Object> item) {
		String value = value(item);
		if (value == null) {
			return null;
		}
		return commands.set(redis, key, value);
	}

	protected abstract String value(Map<String, Object> item);

	@Accessors(fluent = true)
	public static class SetField extends Set {

		@Setter
		private String field;

		@Override
		protected String value(Map<String, Object> item) {
			return convert(item.get(field), String.class);
		}

	}

	@Slf4j
	@Accessors(fluent = true)
	public static class SetObject extends Set {

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

}
