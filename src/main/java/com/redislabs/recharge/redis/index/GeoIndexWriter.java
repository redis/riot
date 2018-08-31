package com.redislabs.recharge.redis.index;

import java.util.Map;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class GeoIndexWriter extends AbstractIndexWriter {

	public GeoIndexWriter(StringRedisTemplate template, EntityConfiguration entity, IndexConfiguration index) {
		super(template, entity, index);
	}

	@Override
	protected void writeIndex(StringRedisConnection conn, String key, String id, Map<String, Object> record) {
		double longitude = convert(record.get(getConfig().getLongitude()), Double.class);
		double latitude = convert(record.get(getConfig().getLatitude()), Double.class);
		conn.geoAdd(key, new Point(longitude, latitude), id);
	}

}
