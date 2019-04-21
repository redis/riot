package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

public interface RedisCommands {

	Object geoadd(Object redis, String key, double longitude, double latitude, String member);

	Object hmset(Object redis, String key, Map<String, Object> item);

	Object rpush(Object redis, String key, String member);

	Object lpush(Object redis, String key, String member);

	Object sadd(Object redis, String key, String member);

	Object set(Object redis, String key, String string);

	Object zadd(Object redis, String key, double score, String member);

	Object xadd(Object redis, String key, Map<String, Object> item, long maxlen, boolean approximateTrimming);

	Object xadd(Object redis, String key, String id, Map<String, Object> item, long maxlen,
			boolean approximateTrimming);

	Object xadd(Object redis, String key, Map<String, Object> item);

	Object xadd(Object redis, String key, String id, Map<String, Object> item);

	Object ftadd(Object redis, String index, String docId, double score, Map<String, Object> item, AddOptions options,
			String payload);

	Object sugadd(Object redis, String index, String string, double score, boolean increment, String payload);

}
