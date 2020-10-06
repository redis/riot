package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import lombok.Setter;

public class Xadd<K, V, T> extends AbstractKeyWriter<K, V, T> {

	@Setter
	private Converter<T, Map<K, V>> bodyConverter;
	@Setter
	private Converter<T, String> idConverter;
	@Setter
	private Long maxlen;
	@Setter
	private boolean approximateTrimming;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<String> write(BaseRedisAsyncCommands<K, V> commands, T item, K key) {
		XAddArgs args = new XAddArgs();
		if (idConverter != null) {
			args.id(idConverter.convert(item));
		}
		if (maxlen != null) {
			args.maxlen(maxlen);
		}
		args.approximateTrimming(approximateTrimming);
		return ((RedisStreamAsyncCommands<K, V>) commands).xadd(key, args, bodyConverter.convert(item));
	}

}
