package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redislabs.recharge.RechargeConfiguration.KeyConfiguration;

public abstract class AbstractRedisWriter extends AbstractItemStreamItemWriter<Map<String, Object>> {

	private KeyBuilder keyBuilder;

	public AbstractRedisWriter(KeyConfiguration keyConfig) {
		this.keyBuilder = new KeyBuilder(keyConfig);
	}

	protected String getKey(Map<String, Object> map) {
		return keyBuilder.getKey(map);
	}

	protected String getId(Map<String, Object> map) {
		return keyBuilder.getId(map);
	}

}
