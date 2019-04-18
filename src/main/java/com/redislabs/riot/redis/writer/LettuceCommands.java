package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

@SuppressWarnings("unchecked")
public class LettuceCommands extends AbstractRedisCommands {

	@Override
	public Object geoadd(Object redis, String key, double longitude, double latitude, String member) {
		return ((RedisAsyncCommands<String, String>) redis).geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(Object redis, String key, Map<String, Object> item) {
		return ((RedisAsyncCommands<String, String>) redis).hmset(key, stringMap(item));
	}

	@Override
	public Object rpush(Object redis, String key, String member) {
		return ((RedisAsyncCommands<String, String>) redis).rpush(key, member);
	}

	@Override
	public Object lpush(Object redis, String key, String member) {
		return ((RedisAsyncCommands<String, String>) redis).lpush(key, member);
	}

	@Override
	public Object sadd(Object redis, String key, String member) {
		return ((RedisAsyncCommands<String, String>) redis).sadd(key, member);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, Object> item, Long maxlen,
			boolean approximateTrimming) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(approximateTrimming);
		if (id != null) {
			args.id(id);
		}
		if (maxlen != null) {
			args.maxlen(maxlen);
		}
		return ((RedisAsyncCommands<String, String>) redis).xadd(key, args, stringMap(item));
	}

	@Override
	public Object set(Object redis, String key, String string) {
		return ((RedisAsyncCommands<String, String>) redis).set(key, string);
	}

	@Override
	public Object zadd(Object redis, String key, double score, String member) {
		return ((RedisAsyncCommands<String, String>) redis).zadd(key, score, member);
	}

}
