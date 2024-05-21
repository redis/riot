package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import com.redis.riot.core.function.ToGeoValueFunction;
import com.redis.spring.batch.writer.Geoadd;

public class GeoaddBuilder extends AbstractCollectionMapOperationBuilder {

	private String longitude;

	private String latitude;

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	@Override
	protected Geoadd<String, String, Map<String, Object>> operation(Function<Map<String, Object>, String> keyFunction) {
		ToDoubleFunction<Map<String, Object>> lon = toDouble(longitude, 0);
		ToDoubleFunction<Map<String, Object>> lat = toDouble(latitude, 0);
		ToGeoValueFunction<String, Map<String, Object>> valueFunction = new ToGeoValueFunction<>(member(), lon, lat);
		return new Geoadd<>(keyFunction, valueFunction);
	}

}
