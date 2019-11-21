package com.redislabs.riot.batch.redis.map;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisAsyncCommands;

public class LettuceAsyncCommands implements RedisCommands<RedisAsyncCommands<String, String>> {

	@Override
	public Object geoadd(RedisAsyncCommands<String, String> redis, String key, double longitude, double latitude,
			String member) {
		return redis.geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(RedisAsyncCommands<String, String> redis, String key, Map<String, String> map) {
		return redis.hmset(key, map);
	}

	@Override
	public Object xadd(RedisAsyncCommands<String, String> redis, String key, Map<String, String> map) {
		return redis.xadd(key, map);
	}

	@Override
	public Object xadd(RedisAsyncCommands<String, String> redis, String key, String id, Map<String, String> map,
			long maxlen, boolean approximateTrimming) {
		return redis.xadd(key, xaddArgs(id).maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	private XAddArgs xaddArgs(String id) {
		return new XAddArgs().id(id);
	}

	@Override
	public Object zadd(RedisAsyncCommands<String, String> redis, String key, double score, String member) {
		return redis.zadd(key, score, member);
	}

	@Override
	public Object xadd(RedisAsyncCommands<String, String> redis, String key, String id, Map<String, String> map) {
		return redis.xadd(key, xaddArgs(id), map);
	}

	@Override
	public Object set(RedisAsyncCommands<String, String> redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(RedisAsyncCommands<String, String> redis, String key, String member) {
		return redis.sadd(key, member);
	}

	@Override
	public Object rpush(RedisAsyncCommands<String, String> redis, String key, String member) {
		return redis.rpush(key, member);
	}

	@Override
	public Object lpush(RedisAsyncCommands<String, String> redis, String key, String member) {
		return redis.lpush(key, member);
	}

	@Override
	public Object expire(RedisAsyncCommands<String, String> redis, String key, long timeout) {
		return redis.expire(key, timeout);
	}

	@Override
	public Object evalsha(RedisAsyncCommands<String, String> redis, String sha, ScriptOutputType type, String[] keys,
			String[] args) {
		return redis.evalsha(sha, type, keys, args);
	}

	@Override
	public Object ftadd(RedisAsyncCommands<String, String> redis, String index, String docId, double score,
			Map<String, String> map, AddOptions options) {
		return ((RediSearchAsyncCommands<String, String>) redis).add(index, docId, score, map, options);
	}

	@Override
	public Object ftadd(RedisAsyncCommands<String, String> redis, String index, String docId, double score,
			Map<String, String> map, AddOptions options, String payload) {
		return ((RediSearchAsyncCommands<String, String>) redis).add(index, docId, score, map, options, payload);
	}

	@Override
	public Object sugadd(RedisAsyncCommands<String, String> redis, String index, String string, double score,
			boolean increment) {
		return ((RediSearchAsyncCommands<String, String>) redis).sugadd(index, string, score, increment);
	}

	@Override
	public Object sugadd(RedisAsyncCommands<String, String> redis, String index, String string, double score,
			boolean increment, String payload) {
		return ((RediSearchAsyncCommands<String, String>) redis).sugadd(index, string, score, increment, payload);
	}
}
