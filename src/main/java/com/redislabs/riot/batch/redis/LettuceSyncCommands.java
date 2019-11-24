package com.redislabs.riot.batch.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;

public class LettuceSyncCommands implements RedisCommands<io.lettuce.core.api.sync.RedisCommands<String, String>> {

	@Override
	public Object geoadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, double longitude,
			double latitude, String member) {
		return redis.geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key,
			Map<String, String> map) {
		return redis.hmset(key, map);
	}

	@Override
	public Object xadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key,
			Map<String, String> map) {
		return redis.xadd(key, map);
	}

	@Override
	public Object xadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, String id,
			Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return redis.xadd(key, new XAddArgs().id(id).maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object xadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, String id,
			Map<String, String> map) {
		return redis.xadd(key, new XAddArgs().id(id), map);
	}

	@Override
	public Object xadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key,
			Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return redis.xadd(key, new XAddArgs().maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object zadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, double score,
			String member) {
		return redis.zadd(key, score, member);
	}

	@Override
	public Object set(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, String member) {
		return redis.sadd(key, member);
	}

	@Override
	public Object rpush(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, String member) {
		return redis.rpush(key, member);
	}

	@Override
	public Object lpush(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, String member) {
		return redis.lpush(key, member);
	}

	@Override
	public Object expire(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String key, long timeout) {
		return redis.expire(key, timeout);
	}

	@Override
	public Object evalsha(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String sha,
			ScriptOutputType type, String[] keys, String[] args) {
		return redis.evalsha(sha, type, keys, args);
	}

	@Override
	public Object ftadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String index, String docId,
			double score, Map<String, String> map, AddOptions options) {
		return ((RediSearchCommands<String, String>) redis).add(index, docId, score, map, options);
	}

	@Override
	public Object ftadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String index, String docId,
			double score, Map<String, String> map, AddOptions options, String payload) {
		return ((RediSearchCommands<String, String>) redis).add(index, docId, score, map, options, payload);
	}

	@Override
	public Object sugadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String index, String string,
			double score, boolean increment) {
		return ((RediSearchCommands<String, String>) redis).sugadd(index, string, score, increment);
	}

	@Override
	public Object sugadd(io.lettuce.core.api.sync.RedisCommands<String, String> redis, String index, String string,
			double score, boolean increment, String payload) {
		return ((RediSearchCommands<String, String>) redis).sugadd(index, string, score, increment, payload);
	}

}
