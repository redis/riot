package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.CollectionRedisWriterConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public abstract class AbstractCollectionRedisWriter extends AbstractRedisWriter {

	private CollectionRedisWriterConfiguration collectionConfig;

	protected AbstractCollectionRedisWriter(StringRedisTemplate template, RedisWriterConfiguration writer,
			CollectionRedisWriterConfiguration collectionConfig) {
		super(template, writer);
		this.collectionConfig = collectionConfig;
	}
	
	public CollectionRedisWriterConfiguration getCollectionConfig() {
		return collectionConfig;
	}

	protected String getMemberId(Map<String, Object> record) {
		return getValues(record, collectionConfig.getFields());
	}

}
