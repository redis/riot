package com.redislabs.riot.redis;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.core.convert.converter.Converter;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import lombok.Setter;

public class Evalsha<K, V, T> extends AbstractRedisItemWriter<K, V, T> {

	@Setter
	private String sha;
	@Setter
	private ScriptOutputType outputType;
	@Setter
	private Converter<T, K[]> keysConverter;
	@Setter
	private Converter<T, V[]> argsConverter;

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(BaseRedisAsyncCommands<K, V> commands, T item) {
		return ((RedisScriptingAsyncCommands<K, V>) commands).evalsha(sha, outputType, keysConverter.convert(item),
				argsConverter.convert(item));
	}

}
