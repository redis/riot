package com.redislabs.riot.batch.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.batch.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@Accessors(fluent = true)
public class Geoadd<R> extends AbstractCollectionMapWriter<R> {

	@Setter
	private String longitudeField;
	@Setter
	private String latitudeField;

	@Override
	protected Object write(RedisCommands<R> commands, R redis, String key, String member, Map<String, Object> item) {
		Double longitude = coordinate(item, longitudeField);
		if (longitude == null) {
			return null;
		}
		Double latitude = coordinate(item, latitudeField);
		if (latitude == null) {
			return null;
		}
		return commands.geoadd(redis, key, longitude, latitude, member);
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

}
