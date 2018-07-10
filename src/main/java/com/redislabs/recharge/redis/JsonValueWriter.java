package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.recharge.config.KeyConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JsonValueWriter extends AbstractTemplateWriter {

	private ObjectMapper mapper = new ObjectMapper();

	public JsonValueWriter(KeyConfiguration keyConfig, StringRedisTemplate template) {
		super(keyConfig, template);
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> map) {
		try {
			String json = mapper.writeValueAsString(map);
			conn.set(getKey(map), json);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize to JSON", e);
		}
	}

}
