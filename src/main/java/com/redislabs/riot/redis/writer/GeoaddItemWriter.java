package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class GeoaddItemWriter extends CollectionItemWriter {

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

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		Double longitude = longitude(item);
		Double latitude = latitude(item);
		if (longitude == null || latitude == null) {
			return null;
		}
		return commands.geoadd(key, longitude, latitude, member);
	}

}
