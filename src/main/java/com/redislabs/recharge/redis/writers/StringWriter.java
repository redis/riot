package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.RechargeConfiguration.StringConfiguration;

import io.lettuce.core.RedisFuture;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SuppressWarnings("rawtypes")
public class StringWriter extends AbstractPipelineRedisWriter<StringConfiguration> {

	private ObjectWriter objectWriter;

	public StringWriter(StringConfiguration config) {
		super(config);
		this.objectWriter = config.getXml() == null ? new ObjectMapper().writer()
				: new XmlMapper().writer().withRootName(config.getXml().getRootName());
	}

	@Override
	protected RedisFuture<String> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
		try {
			String string = objectWriter.writeValueAsString(record);
			return commands.set(key, string);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize value to JSON: {}", record, e);
			return null;
		}
	}

}
