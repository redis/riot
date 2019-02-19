package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.recharge.redis.GeoConfiguration;

import io.lettuce.core.RedisFuture;

@SuppressWarnings("rawtypes")
public class GeoAddWriter extends AbstractPipelineRedisWriter<GeoConfiguration> {

	public GeoAddWriter(GeoConfiguration config) {
		super(config);
	}

	@Override
	protected RedisFuture<?> write(String key, Map record, RediSearchAsyncCommands<String, String> commands) {
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
		String member = getValues(record, config.getFields());
		return commands.geoadd(key, lon, lat, member);
	}

}
