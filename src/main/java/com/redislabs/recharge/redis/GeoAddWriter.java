package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.GeoConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class GeoAddWriter extends AbstractCollectionRedisWriter {

	private GeoConfiguration geo;

	public GeoAddWriter(StringRedisTemplate template, RedisWriterConfiguration writer) {
		super(template, writer, writer.getGeo());
		this.geo = writer.getGeo();
	}

	@Override
	protected void write(StringRedisConnection conn, String key, Map<String, Object> record) {
		Object longitude = record.get(geo.getLongitude());
		if (longitude == null || longitude.equals("")) {
			return;
		}
		Object latitude = record.get(geo.getLatitude());
		if (latitude == null || latitude.equals("")) {
			return;
		}
		conn.geoAdd(key, new Point(toDouble(longitude), toDouble(latitude)), getMemberId(record));
	}

	private double toDouble(Object object) {
		return convert(object, Double.class);
	}

}
