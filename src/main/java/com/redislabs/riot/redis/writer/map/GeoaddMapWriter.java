package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class GeoaddMapWriter extends CollectionMapWriter {

	private String longitudeField;
	private String latitudeField;

	public void setLatitudeField(String latitudeField) {
		this.latitudeField = latitudeField;
	}

	public void setLongitudeField(String longitudeField) {
		this.longitudeField = longitudeField;
	}

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		Double longitude = longitude(item);
		Double latitude = latitude(item);
		if (longitude == null || latitude == null) {
			return null;
		}
		return pipeline.geoadd(key, longitude, latitude, member);
	}

	@Override
	protected void write(JedisCluster cluster, String key, String member, Map<String, Object> item) {
		Double longitude = longitude(item);
		Double latitude = latitude(item);
		if (longitude == null || latitude == null) {
			return;
		}
		cluster.geoadd(key, longitude, latitude, member);
	}

	private Double coordinate(Map<String, Object> item, String field) {
		if (item.containsKey(field)) {
			Object value = item.get(field);
			if (value != null && !"".equals(value)) {
				return convert(value, Double.class);
			}
		}
		return null;
	}

	private Double longitude(Map<String, Object> item) {
		return coordinate(item, longitudeField);
	}

	private Double latitude(Map<String, Object> item) {
		return coordinate(item, latitudeField);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, String member, Map<String, Object> item) {
		Double longitude = longitude(item);
		Double latitude = latitude(item);
		if (longitude == null || latitude == null) {
			return null;
		}
		return ((RedisGeoAsyncCommands<String, String>) commands).geoadd(key, longitude, latitude, member);
	}

}
