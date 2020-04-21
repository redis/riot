package com.redislabs.riot.redis;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.lettusearch.search.AddOptions;

import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.suggest.Suggestion;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;

public interface RedisCommands<T> {

    Object geoadd(T redis, String key, double longitude, double latitude, String member);

    Object hmset(T redis, String key, Map<String, String> map);

    Object zadd(T redis, String key, double score, String member);

    Object xadd(T redis, String key, Map<String, String> map);

    Object xadd(T redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming);

    Object xadd(T redis, String key, String id, Map<String, String> map);

    Object xadd(T redis, String key, String id, Map<String, String> map, long maxlen, boolean approximateTrimming);

    Object xadd(T redis, String key, List<String> ids, List<Map<String, String>> maps);

    Object set(T redis, String key, String value);

    Object sadd(T redis, String key, String... members);

    Object rpush(T redis, String key, String... members);

    Object lpush(T redis, String key, String... members);

    Object expire(T redis, String key, long timeout);

    Object evalsha(T redis, String sha, ScriptOutputType type, String[] keys, String[] args);

    Object ftadd(T redis, String index, Document<String, String> document, AddOptions options);

    Object ftsearch(T redis, String index, String query, Object... options);

    Object ftaggregate(T redis, String index, String query, Object... options);

    Object ftaggregate(T redis, String index, String query, Cursor cursor, Object... options);

    Object sugadd(T redis, String key, Suggestion<String> suggestion, boolean increment);

    Object restore(T redis, String key, byte[] value, long ttl, boolean replace);

    Object del(T redis, String... keys);

    Object zadd(T redis, String key, List<ScoredValue<String>> scoredValues);

}
