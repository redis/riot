package com.redislabs.recharge.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import lombok.Setter;

@Setter
public class StreamWriter extends AbstractSingleRedisWriter {

	private Long maxlen;
	private boolean approximateTrimming;
	private String idField;

	@Override
	protected RedisFuture<?> writeSingle(String key, Map<String, Object> record,
			RediSearchAsyncCommands<String, String> commands) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(approximateTrimming);
		if (idField != null) {
			args.id(converter.convert(record.getOrDefault(idField, idField), String.class));
		}
		if (maxlen != null) {
			args.maxlen(maxlen);
		}
		return commands.xadd(key, args, toStringMap(record));
	}

}