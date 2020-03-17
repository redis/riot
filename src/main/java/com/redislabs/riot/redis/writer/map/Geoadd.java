package com.redislabs.riot.redis.writer.map;

import java.util.Map;

import com.redislabs.riot.redis.RedisCommands;

import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings({ "rawtypes", "unchecked" })
@Accessors(fluent = true)
public class Geoadd extends AbstractCollectionMapCommandWriter {

	@Setter
	private String longitude;
	@Setter
	private String latitude;

	@Override
	protected Object write(RedisCommands commands, Object redis, String key, String member, Map<String, Object> item) {
		Double lon = coordinate(item, longitude);
		if (lon == null) {
			return null;
		}
		Double lat = coordinate(item, latitude);
		if (lat == null) {
			return null;
		}
		return commands.geoadd(redis, key, lon, lat, member);
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
