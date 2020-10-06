package com.redislabs.riot.redis;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisGeoAsyncCommands;
import lombok.Setter;

public class Geoadd<K, V, T> extends AbstractCollectionWriter<K, V, T> {

	@Setter
	private Converter<T, Double> longitudeConverter;
	@Setter
	private Converter<T, Double> latitudeConverter;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item, K key, V memberId) {
		Double longitude = longitudeConverter.convert(item);
		if (longitude == null) {
			return null;
		}
		Double latitude = latitudeConverter.convert(item);
		if (latitude == null) {
			return null;
		}
		return ((RedisGeoAsyncCommands<K, V>) commands).geoadd(key, longitude, latitude, memberId);
	}

}
