package com.redislabs.recharge.redis.writers;

import java.util.Map;

import com.redislabs.recharge.RechargeConfiguration.GeoConfiguration;

@SuppressWarnings("rawtypes")
public class GeoAddWriter extends AbstractPipelineRedisWriter<GeoConfiguration> {

	public GeoAddWriter(GeoConfiguration config) {
		super(config);
	}

	@Override
	protected void write(String key, Map record) {
		Object longitude = record.get(config.getLongitude());
		if (longitude == null || longitude.equals("")) {
			return;
		}
		Object latitude = record.get(config.getLatitude());
		if (latitude == null || latitude.equals("")) {
			return;
		}
		double lon = converter.convert(longitude, Double.class);
		double lat = converter.convert(latitude, Double.class);
		commands.geoadd(key, lon, lat, getValues(record, config.getFields()));
	}

}
