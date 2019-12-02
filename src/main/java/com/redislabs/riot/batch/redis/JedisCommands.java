package com.redislabs.riot.batch.redis;

import java.util.Arrays;
import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

public class JedisCommands implements RedisCommands<Jedis> {

	@Override
	public Object geoadd(Jedis redis, String key, double longitude, double latitude, String member) {
		return redis.geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(Jedis redis, String key, Map<String, String> map) {
		return redis.hmset(key, map);
	}

	@Override
	public Object zadd(Jedis redis, String key, double score, String member) {
		return redis.zadd(key, score, member);
	}

	@Override
	public Object xadd(Jedis redis, String key, Map<String, String> map) {
		return redis.xadd(key, null, map);
	}

	@Override
	public Object xadd(Jedis redis, String key, String id, Map<String, String> map) {
		return redis.xadd(key, new StreamEntryID(id), map);
	}

	@Override
	public Object xadd(Jedis redis, String key, String id, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return redis.xadd(key, new StreamEntryID(id), map, maxlen, approximateTrimming);
	}

	@Override
	public Object xadd(Jedis redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return redis.xadd(key, null, map, maxlen, approximateTrimming);
	}

	@Override
	public Object set(Jedis redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(Jedis redis, String key, String member) {
		return redis.sadd(key, member);
	}

	@Override
	public Object rpush(Jedis redis, String key, String member) {
		return redis.rpush(key, member);
	}

	@Override
	public Object lpush(Jedis redis, String key, String member) {
		return redis.lpush(key, member);
	}

	@Override
	public Object expire(Jedis redis, String key, long timeout) {
		return redis.expire(key, Math.toIntExact(timeout));
	}

	@Override
	public Object evalsha(Jedis redis, String sha, ScriptOutputType type, String[] keys, String[] args) {
		return redis.evalsha(sha, Arrays.asList(keys), Arrays.asList(args));
	}

	@Override
	public Object restore(Jedis redis, String key, long ttl, byte[] value) {
		return redis.restore(key, Math.toIntExact(ttl), value);
	}

	@Override
	public Object ftadd(Jedis redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}

	@Override
	public Object ftadd(Jedis redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options, String payload) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}

	@Override
	public Object sugadd(Jedis redis, String index, String string, double score, boolean increment) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}

	@Override
	public Object sugadd(Jedis redis, String index, String string, double score, boolean increment, String payload) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}

}
