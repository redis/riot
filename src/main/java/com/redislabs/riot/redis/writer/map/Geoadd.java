package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Geoadd extends AbstractCollectionMapWriter {

	@Setter
	private String longitudeField;
	@Setter
	private String latitudeField;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
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
