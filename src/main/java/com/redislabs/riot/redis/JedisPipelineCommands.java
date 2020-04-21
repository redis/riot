package com.redislabs.riot.redis;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.lettusearch.search.AddOptions;

import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.suggest.Suggestion;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;

public class JedisPipelineCommands implements RedisCommands<Pipeline> {

	@Override
	public Object del(Pipeline redis, String... keys) {
		return redis.del(keys);
	}

	@Override
	public Object geoadd(Pipeline redis, String key, double longitude, double latitude, String member) {
		return redis.geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(Pipeline redis, String key, Map<String, String> map) {
		return redis.hmset(key, map);
	}

	@Override
	public Object zadd(Pipeline redis, String key, double score, String member) {
		return redis.zadd(key, score, member);
	}

	@Override
	public Object zadd(Pipeline redis, String key, List<ScoredValue<String>> scoredValues) {
		Map<String, Double> scoreMembers = new HashMap<>();
		scoredValues.forEach(v -> scoreMembers.put(v.getValue(), v.getScore()));
		return redis.zadd(key, scoreMembers);
	}

	@Override
	public Object xadd(Pipeline redis, String key, Map<String, String> map) {
		return redis.xadd(key, null, map);
	}

	@Override
	public Object xadd(Pipeline redis, String key, String id, Map<String, String> map) {
		return redis.xadd(key, new StreamEntryID(id), map);
	}

	@Override
	public Object xadd(Pipeline redis, String key, String id, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return redis.xadd(key, new StreamEntryID(id), map, maxlen, approximateTrimming);
	}

	@Override
	public Object xadd(Pipeline redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return redis.xadd(key, null, map, maxlen, approximateTrimming);
	}

	@Override
	public Object xadd(Pipeline redis, String key, List<String> ids, List<Map<String, String>> maps) {
		throw new UnsupportedOperationException("Multi-message xadd not supported");
	}

	@Override
	public Object set(Pipeline redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(Pipeline redis, String key, String... members) {
		return redis.sadd(key, members);
	}

	@Override
	public Object rpush(Pipeline redis, String key, String... members) {
		return redis.rpush(key, members);
	}

	@Override
	public Object lpush(Pipeline redis, String key, String... members) {
		return redis.lpush(key, members);
	}

	@Override
	public Object expire(Pipeline redis, String key, long timeout) {
		return redis.expire(key, Math.toIntExact(timeout));
	}

	@Override
	public Object evalsha(Pipeline redis, String sha, ScriptOutputType type, String[] keys, String[] args) {
		return redis.evalsha(sha, Arrays.asList(keys), Arrays.asList(args));
	}

	@Override
	public Object restore(Pipeline redis, String key, byte[] value, long ttl, boolean replace) {
		int intTtl = Math.toIntExact(ttl);
		if (replace) {
			return redis.restoreReplace(key, intTtl, value);
		}
		return redis.restore(key, intTtl, value);
	}

	@Override
	public Object ftadd(Pipeline redis, String index, Document<String, String> document, AddOptions options) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object sugadd(Pipeline redis, String key, Suggestion<String> suggestion, boolean increment) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object ftsearch(Pipeline redis, String index, String query, Object... options) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object ftaggregate(Pipeline redis, String index, String query, Cursor cursor, Object... options) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object ftaggregate(Pipeline redis, String index, String query, Object... options) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

}
