package com.redislabs.riot.batch.redis;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;

public interface RedisCommands<T> {

	Object geoadd(T redis, String key, double longitude, double latitude, String member);

	Object hmset(T redis, String key, Map<String, String> map);

	Object zadd(T redis, String key, double score, String member);

	Object xadd(T redis, String key, Map<String, String> map);

	Object xadd(T redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming);

	Object xadd(T redis, String key, String id, Map<String, String> map);

	Object xadd(T redis, String key, String id, Map<String, String> map, long maxlen, boolean approximateTrimming);

	Object set(T redis, String key, String value);

	Object sadd(T redis, String key, String member);

	Object rpush(T redis, String key, String member);

	Object lpush(T redis, String key, String member);

	Object expire(T redis, String key, long timeout);

	Object evalsha(T redis, String sha, ScriptOutputType type, String[] keys, String[] args);

	Object ftadd(T redis, String index, String docId, double score, Map<String, String> map, AddOptions options);

	Object ftadd(T redis, String index, String docId, double score, Map<String, String> map, AddOptions options,
			String payload);

	Object sugadd(T redis, String index, String string, double score, boolean increment);

	Object sugadd(T redis, String index, String string, double score, boolean increment, String payload);

	Object restore(T redis, String key, long ttl, byte[] value);

}
