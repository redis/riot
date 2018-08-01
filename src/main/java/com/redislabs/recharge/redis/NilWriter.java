package com.redislabs.recharge.redis;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NilWriter extends AbstractEntityWriter {

	private int currentItemCount = 0;

	public NilWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity) {
		super(template, entity);
	}

	@Override
	public void write(List<? extends Map<String, Object>> items) {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key) {
		// not called
	}
}
