package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringWriter extends AbstractRedisSimpleWriter {

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
	protected RedisFuture<?> writeSingle(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		try {
			return commands.set(key, objectWriter.writeValueAsString(record));
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value: {}", record, e);
			return null;
		}
	}

	public static enum StringFormat {
		Xml, Json
	}

}
