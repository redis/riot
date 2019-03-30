package com.redislabs.recharge.redis.writer;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.RedisType;

import io.lettuce.core.RedisFuture;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Setter
public class StringWriter extends AbstractSingleRedisWriter implements InitializingBean {

	private ObjectWriter objectWriter;

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

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(objectWriter, "ObjectWriter not specified");
	}

	@Override
	public RedisType getRedisType() {
		return RedisType.String;
	}

}
