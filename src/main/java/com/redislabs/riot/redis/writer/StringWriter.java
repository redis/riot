package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.riot.redis.RedisConverter;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringWriter implements RedisItemWriter {

	@Setter
	protected RedisConverter converter;
	@Setter
	protected RedisCommands commands;

	public static enum StringFormat {
		Xml, Json
	}

	private ObjectWriter objectWriter;

	public ObjectWriter setFormat(StringFormat format) {
		objectWriter = objectWriter(format);
		return objectWriter;
	}

	private ObjectWriter objectWriter(StringFormat format) {
		if (format == StringFormat.Xml) {
			return new XmlMapper().writer();
		}
		return new ObjectMapper().writer();
	}

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		String key = converter.key(item);
		try {
			return commands.set(redis, key, objectWriter.writeValueAsString(item));
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value: {}", item, e);
			return null;
		}
	}

}
