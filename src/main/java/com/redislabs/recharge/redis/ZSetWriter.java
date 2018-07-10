package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.KeyConfiguration;
import com.redislabs.recharge.RechargeConfiguration.ZSetConfiguration;

public class ZSetWriter extends AbstractCollectionWriter {

	private ConversionService conversionService = new DefaultConversionService();
	private ZSetConfiguration config;

	public ZSetWriter(KeyConfiguration keyConfig, StringRedisTemplate template, ZSetConfiguration config) {
		super(keyConfig, template, config.getKey());
		this.config = config;
	}

	@Override
	protected void write(StringRedisConnection conn, String collectionKey, String id, Map<String, Object> map) {
		double score = conversionService.convert(map.get(config.getScore()), Double.class);
		conn.zAdd(collectionKey, score, id);
	}

}
