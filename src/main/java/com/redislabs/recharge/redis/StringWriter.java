package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringWriter extends AbstractEntityWriter {

	private ObjectWriter writer;

	public StringWriter(StringRedisTemplate template, EntityConfiguration entity, ObjectWriter writer) {
		super(template, entity);
		this.writer = writer;
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		try {
			String value = writer.writeValueAsString(record);
			conn.set(key, value);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize values", e);
		}
	}

}
