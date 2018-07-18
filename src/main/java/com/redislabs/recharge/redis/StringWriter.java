package com.redislabs.recharge.redis;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringWriter extends AbstractTemplateWriter {

	private ObjectWriter writer;

	public StringWriter(EntityConfiguration entityConfig, StringRedisTemplate template, ObjectWriter writer) {
		super(entityConfig, template);
		this.writer = writer;
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id) {
		try {
			String json = writer.writeValueAsString(entity.getFields());
			conn.set(getKey(entity, id), json);
		} catch (JsonProcessingException e) {
			log.error("Could not serialize values", e);
		}
	}

}
