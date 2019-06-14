package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import lombok.Value;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class GeoWriter extends AbstractCollectionRedisItemWriter {

	@Value
	private static class Point {
		private Double longitude;
		private Double latitude;

		public boolean isInvalid() {
			return longitude == null || latitude == null;
		}

	}

	@Setter
	private String longitudeField;
	@Setter
	private String latitudeField;

	@Override
	protected Response<Long> write(Pipeline pipeline, String key, String member, Map<String, Object> item) {
		Point point = point(item);
		if (point.isInvalid()) {
			return null;
		}
		return pipeline.geoadd(key, point.getLongitude(), point.getLatitude(), member);
	}

	private Point point(Map<String, Object> item) {
		return new Point(longitude(item), latitude(item));
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
		Point point = point(item);
		if (point.isInvalid()) {
			return null;
		}
		return commands.geoadd(key, point.getLongitude(), point.getLatitude(), member);
	}

}
