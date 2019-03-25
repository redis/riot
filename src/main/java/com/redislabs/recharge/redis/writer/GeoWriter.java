package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import lombok.Setter;

@Setter
public class GeoWriter extends AbstractCollectionRedisWriter {

	private String longitudeField;
	private String latitudeField;

	@Override
	protected RedisFuture<?> write(String key, String member, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		Object longitude = record.get(longitudeField);
		if (longitude == null || longitude.equals("")) {
			return null;
		}
		Object latitude = record.get(latitudeField);
		if (latitude == null || latitude.equals("")) {
			return null;
		}
		double lon = converter.convert(longitude, Double.class);
		double lat = converter.convert(latitude, Double.class);
		return commands.geoadd(key, lon, lat, member);
	}

}
