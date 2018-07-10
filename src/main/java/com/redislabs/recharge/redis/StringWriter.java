package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redislabs.recharge.RechargeConfiguration.KeyConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringWriter extends AbstractTemplateWriter {

	private ObjectWriter writer;

	public StringWriter(ObjectWriter writer, KeyConfiguration keyConfig, StringRedisTemplate template) {
		super(keyConfig, template);
		this.writer = writer;
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> map) {
		try {
			String json = writer.writeValueAsString(map);
			conn.set(getKey(map), json);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize values", e);
		}
	}

}
