package com.redislabs.recharge.redis.string;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.recharge.redis.SingleRedisWriter;

import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public class StringWriter extends SingleRedisWriter<StringConfiguration> {

	private ObjectWriter objectWriter;

	public StringWriter(StringConfiguration config,
			GenericObjectPool<StatefulRediSearchConnection<String, String>> pool) {
		super(config, pool);
		this.objectWriter = writer(config);
	}

	private ObjectWriter writer(StringConfiguration config) {
		switch (config.getFormat()) {
		case xml:
			return new XmlMapper().writer().withRootName(config.getXml().getRoot());
		default:
			return new ObjectMapper().writer();
		}
	}

	@Override
	protected RedisFuture<?> writeSingle(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		try {
			return commands.set(key, objectWriter.writeValueAsString(record));
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value: {}", record, e);
			return null;
		}
	}

}
