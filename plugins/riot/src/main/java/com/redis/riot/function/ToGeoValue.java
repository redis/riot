package com.redis.riot.function;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import io.lettuce.core.GeoValue;

public class ToGeoValue<V, T> implements Function<T, GeoValue<V>> {

	private final Function<T, V> memberConverter;
	private final ToDoubleFunction<T> longitudeConverter;
	private final ToDoubleFunction<T> latitudeConverter;

	public ToGeoValue(Function<T, V> member, ToDoubleFunction<T> longitude, ToDoubleFunction<T> latitude) {
		this.memberConverter = member;
		this.longitudeConverter = longitude;
		this.latitudeConverter = latitude;
	}

	@Override
	public GeoValue<V> apply(T t) {
		double longitude = this.longitudeConverter.applyAsDouble(t);
		double latitude = this.latitudeConverter.applyAsDouble(t);
		return GeoValue.just(longitude, latitude, memberConverter.apply(t));
	}

}
