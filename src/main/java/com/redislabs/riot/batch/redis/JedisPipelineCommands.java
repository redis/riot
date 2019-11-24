package com.redislabs.riot.batch.redis;

import java.util.Arrays;
import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;

public class JedisPipelineCommands implements RedisCommands<Pipeline> {

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
	public Object set(Pipeline redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(Pipeline redis, String key, String member) {
		return redis.sadd(key, member);
	}

	@Override
	public Object rpush(Pipeline redis, String key, String member) {
		return redis.rpush(key, member);
	}

	@Override
	public Object lpush(Pipeline redis, String key, String member) {
		return redis.lpush(key, member);
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
	public Object ftadd(Pipeline redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object ftadd(Pipeline redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options, String payload) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object sugadd(Pipeline redis, String index, String string, double score, boolean increment) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

	@Override
	public Object sugadd(Pipeline redis, String index, String string, double score, boolean increment, String payload) {
		throw new UnsupportedOperationException("Pipeline not supported with JRediSearch client");
	}

}
