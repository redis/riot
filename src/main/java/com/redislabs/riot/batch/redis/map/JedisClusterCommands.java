package com.redislabs.riot.batch.redis.map;

import java.util.Arrays;
import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.ScriptOutputType;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.StreamEntryID;

public class JedisClusterCommands implements RedisCommands<JedisCluster> {

	@Override
	public Object geoadd(JedisCluster redis, String key, double longitude, double latitude, String member) {
		return redis.geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(JedisCluster redis, String key, Map<String, String> map) {
		return redis.hmset(key, map);
	}

	@Override
	public Object zadd(JedisCluster redis, String key, double score, String member) {
		return redis.zadd(key, score, member);
	}

	@Override
	public Object xadd(JedisCluster redis, String key, Map<String, String> map) {
		return redis.xadd(key, null, map);
	}

	@Override
	public Object xadd(JedisCluster redis, String key, String id, Map<String, String> map) {
		return redis.xadd(key, new StreamEntryID(id), map);
	}

	@Override
	public Object xadd(JedisCluster redis, String key, String id, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return redis.xadd(key, new StreamEntryID(id), map, maxlen, approximateTrimming);
	}

	@Override
	public Object set(JedisCluster redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(JedisCluster redis, String key, String member) {
		return redis.sadd(key, member);
	}

	@Override
	public Object rpush(JedisCluster redis, String key, String member) {
		return redis.rpush(key, member);
	}

	@Override
	public Object lpush(JedisCluster redis, String key, String member) {
		return redis.lpush(key, member);
	}

	@Override
	public Object expire(JedisCluster redis, String key, long timeout) {
		return redis.expire(key, Math.toIntExact(timeout));
	}

	@Override
	public Object evalsha(JedisCluster redis, String sha, ScriptOutputType type, String[] keys, String[] args) {
		return redis.evalsha(sha, Arrays.asList(keys), Arrays.asList(args));
	}

	@Override
	public Object ftadd(JedisCluster redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options) {
		throw new UnsupportedOperationException("Jedis Cluster not supported with RediSearch");
	}

	@Override
	public Object ftadd(JedisCluster redis, String index, String docId, double score, Map<String, String> map,
			AddOptions options, String payload) {
		throw new UnsupportedOperationException("Jedis Cluster not supported with RediSearch");
	}

	@Override
	public Object sugadd(JedisCluster redis, String index, String string, double score, boolean increment) {
		throw new UnsupportedOperationException("Jedis Cluster not supported with RediSearch");
	}

	@Override
	public Object sugadd(JedisCluster redis, String index, String string, double score, boolean increment,
			String payload) {
		throw new UnsupportedOperationException("Jedis Cluster not supported with RediSearch");
	}

}
