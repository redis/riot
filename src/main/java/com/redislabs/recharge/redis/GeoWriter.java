package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.GeoConfiguration;
import com.redislabs.recharge.RechargeConfiguration.KeyConfiguration;

public class GeoWriter extends AbstractCollectionWriter {

	private ConversionService conversionService = new DefaultConversionService();
	private GeoConfiguration config;

	public GeoWriter(KeyConfiguration keyConfig, StringRedisTemplate template, GeoConfiguration config) {
		super(keyConfig, template, config.getKey());
	}

	@Override
	protected void write(StringRedisConnection conn, String collectionKey, String id, Map<String, Object> map) {
		double x = conversionService.convert(map.get(config.getXField()), Double.class);
		double y = conversionService.convert(map.get(config.getYField()), Double.class);
		Point point = new Point(x, y);
		conn.geoAdd(collectionKey, point, id);
	}

}
