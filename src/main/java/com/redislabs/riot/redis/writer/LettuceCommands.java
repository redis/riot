package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

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
	public Object xadd(Object redis, String key, String id, Map<String, Object> item, long maxlen,
			boolean approximateTrimming) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(approximateTrimming);
		args.id(id);
		args.maxlen(maxlen);
		return ((RedisAsyncCommands<String, String>) redis).xadd(key, args, stringMap(item));
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, Object> item) {
		return ((RedisAsyncCommands<String, String>) redis).xadd(key, stringMap(item));
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, Object> item, long maxlen, boolean approximateTrimming) {
		XAddArgs args = new XAddArgs();
		args.approximateTrimming(approximateTrimming);
		args.maxlen(maxlen);
		return ((RedisAsyncCommands<String, String>) redis).xadd(key, args, stringMap(item));
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, Object> item) {
		XAddArgs args = new XAddArgs();
		args.id(id);
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

	@Override
	public Object ftadd(Object redis, String index, String docId, double score, Map<String, Object> item,
			AddOptions options, String payload) {
		return ((RediSearchAsyncCommands<String, String>) redis).add(index, docId, score, stringMap(item), options,
				payload);
	}

	@Override
	public Object sugadd(Object redis, String index, String string, double score, boolean increment, String payload) {
		return ((RediSearchAsyncCommands<String, String>) redis).sugadd(index, string, score, increment, payload);
	}

}
