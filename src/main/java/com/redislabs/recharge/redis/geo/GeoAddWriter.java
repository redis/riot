package com.redislabs.recharge.redis.geo;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.CollectionRedisWriter;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class GeoAddWriter extends CollectionRedisWriter<GeoConfiguration> {

	public GeoAddWriter(GeoConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String key, String member, Map record,
			RediSearchAsyncCommands<String, String> commands) {
		Object longitude = record.get(config.getLongitude());
		if (longitude == null || longitude.equals("")) {
			return null;
		}
		Object latitude = record.get(config.getLatitude());
		if (latitude == null || latitude.equals("")) {
			return null;
		}
		double lon = converter.convert(longitude, Double.class);
		double lat = converter.convert(latitude, Double.class);
		return commands.geoadd(key, lon, lat, member);
	}

}
