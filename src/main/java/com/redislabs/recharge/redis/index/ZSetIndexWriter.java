package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class ZSetIndexWriter extends AbstractIndexWriter {

	private String scoreField;
	private ConversionService converter = new DefaultConversionService();

	public ZSetIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			IndexConfiguration config) {
		super(template, entity, config);
		this.scoreField = config.getScore();
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record, String id) {
		Double score = converter.convert(record.get(scoreField), Double.class);
		conn.zAdd(key, score, id);
	}

	@Override
	protected String getDefaultKeyspace() {
		return scoreField;
	}

}
