package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class GeoWriter extends AbstractCollectionRedisItemWriter {

	@Setter
	private String longitudeField;
	@Setter
	private String latitudeField;

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		Object longitude = item.get(longitudeField);
		if (longitude == null || longitude.equals("")) {
			return null;
		}
		Object latitude = item.get(latitudeField);
		if (latitude == null || latitude.equals("")) {
			return null;
		}
		double lon = convert(longitude, Double.class);
		double lat = convert(latitude, Double.class);
		return pipeline.geoadd(key, lon, lat, member);
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, String member,
			Map<String, Object> item) {
		Object longitude = item.get(longitudeField);
		if (longitude == null || longitude.equals("")) {
			return null;
		}
		Object latitude = item.get(latitudeField);
		if (latitude == null || latitude.equals("")) {
			return null;
		}
		double lon = convert(longitude, Double.class);
		double lat = convert(latitude, Double.class);
		return commands.geoadd(key, lon, lat, member);
	}

}
