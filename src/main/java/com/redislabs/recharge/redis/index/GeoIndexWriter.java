package com.redislabs.recharge.redis.index;

import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.Entity;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;

public class GeoIndexWriter extends AbstractIndexWriter {

	public GeoIndexWriter(EntityConfiguration entityConfig, StringRedisTemplate template,
			IndexConfiguration indexConfig) {
		super(entityConfig, template, indexConfig);
	}

	@Override
	protected void write(StringRedisConnection conn, Entity entity, String id, String indexKey) {
		double x = getValue(entity, getConfig().getLongitude(), Double.class);
		double y = getValue(entity, getConfig().getLatitude(), Double.class);
		Point point = new Point(x, y);
		conn.geoAdd(indexKey, point, id);
	}

}
