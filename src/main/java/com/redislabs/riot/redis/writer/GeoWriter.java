package com.redislabs.riot.redis.writer;

import java.util.Map;

import lombok.Setter;

public class GeoWriter extends AbstractCollectionRedisItemWriter {

	@Setter
	private String longitudeField;
	@Setter
	private String latitudeField;

	@Override
	public Object write(Object redis, Map<String, Object> item) {
		Object longitude = item.get(longitudeField);
		if (longitude == null || longitude.equals("")) {
			return null;
		}
		Object latitude = item.get(latitudeField);
		if (latitude == null || latitude.equals("")) {
			return null;
		}
		double lon = converter.convert(longitude, Double.class);
		double lat = converter.convert(latitude, Double.class);
		return commands.geoadd(redis, converter.key(item), lon, lat, member(item));
	}

}
