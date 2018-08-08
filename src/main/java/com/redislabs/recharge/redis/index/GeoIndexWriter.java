package com.redislabs.recharge.redis.index;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.StringRedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.IndexConfiguration;
import com.redislabs.recharge.redis.key.MultipleFieldKeyBuilder;

public class GeoIndexWriter extends AbstractIndexWriter {

	private String longitudeField;
	private String latitudeField;
	private ConversionService converter = new DefaultConversionService();

	public GeoIndexWriter(StringRedisTemplate template, Entry<String, EntityConfiguration> entity,
			Entry<String, IndexConfiguration> index) {
		super(template, entity, index);
		this.longitudeField = index.getValue().getLongitude();
		this.latitudeField = index.getValue().getLatitude();
	}

	@Override
	protected String getDefaultKeyspace() {
		return MultipleFieldKeyBuilder.join(longitudeField, latitudeField);
	}

	@Override
	protected void write(StringRedisConnection conn, Map<String, Object> record, String id, String key) {
		double longitude = converter.convert(record.get(longitudeField), Double.class);
		double latitude = converter.convert(record.get(latitudeField), Double.class);
		conn.geoAdd(keyspace, new Point(longitude, latitude), id);
	}

}
