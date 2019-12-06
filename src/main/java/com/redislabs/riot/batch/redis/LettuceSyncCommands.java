package com.redislabs.riot.batch.redis;

import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisListCommands;
import io.lettuce.core.api.sync.RedisScriptingCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.api.sync.RedisStringCommands;

@SuppressWarnings("unchecked")
public class LettuceSyncCommands implements RedisCommands<Object> {

	@Override
	public Object geoadd(Object redis, String key, double longitude, double latitude, String member) {
		return ((RedisGeoCommands<String, String>) redis).geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(Object redis, String key, Map<String, String> map) {
		return ((RedisHashCommands<String, String>) redis).hmset(key, map);
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, String> map) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key, map);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key,
				new XAddArgs().id(id).maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, String> map) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key, new XAddArgs().id(id), map);
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key,
				new XAddArgs().maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object zadd(Object redis, String key, double score, String member) {
		return ((RedisSortedSetCommands<String, String>) redis).zadd(key, score, member);
	}

	@Override
	public Object set(Object redis, String key, String value) {
		return ((RedisStringCommands<String, String>) redis).set(key, value);
	}

	@Override
	public Object sadd(Object redis, String key, String member) {
		return ((RedisSetCommands<String, String>) redis).sadd(key, member);
	}

	@Override
	public Object rpush(Object redis, String key, String member) {
		return ((RedisListCommands<String, String>) redis).rpush(key, member);
	}

	@Override
	public Object lpush(Object redis, String key, String member) {
		return ((RedisListCommands<String, String>) redis).lpush(key, member);
	}

	@Override
	public Object expire(Object redis, String key, long timeout) {
		return ((RedisKeyCommands<String, String>) redis).expire(key, timeout);
	}

	@Override
	public Object evalsha(Object redis, String sha, ScriptOutputType type, String[] keys, String[] args) {
		return ((RedisScriptingCommands<String, String>) redis).evalsha(sha, type, keys, args);
	}

	@Override
	public Object restore(Object redis, String key, long ttl, byte[] value) {
		return ((RedisKeyCommands<String, String>) redis).restore(key, ttl, value);
	}

	@Override
	public Object ftadd(Object redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options) {
		return ((RediSearchCommands<String, String>) redis).add(index, docId, score, map, options);
	}

	@Override
	public Object ftadd(Object redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options, String payload) {
		return ((RediSearchCommands<String, String>) redis).add(index, docId, score, map, options, payload);
	}

	@Override
	public Object sugadd(Object redis, String index, String string, double score, boolean increment) {
		return ((RediSearchCommands<String, String>) redis).sugadd(index, string, score, increment);
	}

	@Override
	public Object sugadd(Object redis, String index, String string, double score, boolean increment, String payload) {
		return ((RediSearchCommands<String, String>) redis).sugadd(index, string, score, increment, payload);
	}

}
