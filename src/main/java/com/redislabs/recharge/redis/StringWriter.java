package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.StringConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringWriter extends AbstractRedisWriter {

	private ObjectWriter writer;

	public StringWriter(StringRedisTemplate template, RedisWriterConfiguration config) {
		super(template, config);
		this.writer = getObjectWriter(config.getString());
	}

	private ObjectWriter getObjectWriter(StringConfiguration string) {
		if (string.getXml() != null) {
			return new XmlMapper().writer().withRootName(string.getXml().getRootName());
		}
		return new ObjectMapper().writer();
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
