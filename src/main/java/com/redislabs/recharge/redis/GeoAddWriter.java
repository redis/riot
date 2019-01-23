package com.redislabs.recharge.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.recharge.RechargeConfiguration.GeoConfiguration;
import com.redislabs.recharge.RechargeConfiguration.RedisWriterConfiguration;

public class GeoAddWriter extends AbstractCollectionRedisWriter {

	private GeoConfiguration geo;

	public GeoAddWriter(RediSearchClient client, RedisWriterConfiguration writer) {
		super(client, writer, writer.getGeo());
		this.geo = writer.getGeo();
	}

	@Override
	protected void write(String key, Map<String, Object> record) {
		Object longitude = record.get(geo.getLongitude());
		if (longitude == null || longitude.equals("")) {
			return;
		}
		Object latitude = record.get(geo.getLatitude());
		if (latitude == null || latitude.equals("")) {
			return;
		}
		commands.geoadd(key, toDouble(longitude), toDouble(latitude), getMemberId(record));
	}

	private double toDouble(Object object) {
		return convert(object, Double.class);
	}

}
