package com.redislabs.riot.batch.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

public class LettuceReactiveCommands implements RedisCommands<RedisReactiveCommands<String, String>> {

	@Override
	public Object geoadd(RedisReactiveCommands<String, String> redis, String key, double longitude, double latitude,
			String member) {
		return redis.geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(RedisReactiveCommands<String, String> redis, String key, Map<String, String> map) {
		return redis.hmset(key, map);
	}

	@Override
	public Object xadd(RedisReactiveCommands<String, String> redis, String key, Map<String, String> map) {
		return redis.xadd(key, map);
	}

	@Override
	public Object xadd(RedisReactiveCommands<String, String> redis, String key, String id, Map<String, String> map,
			long maxlen, boolean approximateTrimming) {
		return redis.xadd(key, new XAddArgs().id(id).maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object xadd(RedisReactiveCommands<String, String> redis, String key, String id, Map<String, String> map) {
		return redis.xadd(key, new XAddArgs().id(id), map);
	}

	@Override
	public Object xadd(RedisReactiveCommands<String, String> redis, String key, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return redis.xadd(key, new XAddArgs().maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object zadd(RedisReactiveCommands<String, String> redis, String key, double score, String member) {
		return redis.zadd(key, score, member);
	}

	@Override
	public Object set(RedisReactiveCommands<String, String> redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(RedisReactiveCommands<String, String> redis, String key, String member) {
		return redis.sadd(key, member);
	}

	@Override
	public Object rpush(RedisReactiveCommands<String, String> redis, String key, String member) {
		return redis.rpush(key, member);
	}

	@Override
	public Object lpush(RedisReactiveCommands<String, String> redis, String key, String member) {
		return redis.lpush(key, member);
	}

	@Override
	public Object expire(RedisReactiveCommands<String, String> redis, String key, long timeout) {
		return redis.expire(key, timeout);
	}

	@Override
	public Object evalsha(RedisReactiveCommands<String, String> redis, String sha, ScriptOutputType type, String[] keys,
			String[] args) {
		return redis.evalsha(sha, type, keys, args);
	}
	
	@Override
	public Object restore(RedisReactiveCommands<String, String> redis, String key, long ttl, byte[] value) {
		return redis.restore(key, ttl, value);
	}

	@Override
	public Object ftadd(RedisReactiveCommands<String, String> redis, String index, String docId, double score,
			Map<String, String> map, AddOptions options) {
		return ((RediSearchReactiveCommands<String, String>) redis).add(index, docId, score, map, options);
	}

	@Override
	public Object ftadd(RedisReactiveCommands<String, String> redis, String index, String docId, double score,
			Map<String, String> map, AddOptions options, String payload) {
		return ((RediSearchReactiveCommands<String, String>) redis).add(index, docId, score, map, options, payload);
	}

	@Override
	public Object sugadd(RedisReactiveCommands<String, String> redis, String index, String string, double score,
			boolean increment) {
		return ((RediSearchReactiveCommands<String, String>) redis).sugadd(index, string, score, increment);
	}

	@Override
	public Object sugadd(RedisReactiveCommands<String, String> redis, String index, String string, double score,
			boolean increment, String payload) {
		return ((RediSearchReactiveCommands<String, String>) redis).sugadd(index, string, score, increment, payload);
	}

}
