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
		Object longitude = record.get(getConfig().getLongitude());
		if (longitude == null || longitude.equals("")) {
			return;
		}
		Object latitude = record.get(getConfig().getLatitude());
		if (latitude == null || latitude.equals("")) {
			return;
		}
		conn.geoAdd(key, new Point(toDouble(longitude), toDouble(latitude)), id);
	}

	private double toDouble(Object object) {
		return convert(object, Double.class);
	}

}
