package com.redislabs.riot.redis.writer;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.AccessLevel;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class StringWriter extends AbstractRedisSimpleWriter implements InitializingBean {

	private StringFormat format;
	private String root;

	@Setter(AccessLevel.NONE)
	private ObjectWriter objectWriter;

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(format, "Format not specified");
		this.objectWriter = objectWriter();
	}

	private ObjectWriter objectWriter() {
		if (format == StringFormat.Xml) {
			return new XmlMapper().writer().withRootName(root);
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
