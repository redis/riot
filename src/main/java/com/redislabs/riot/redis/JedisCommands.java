package com.redislabs.riot.redis;

import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.suggest.Suggestion;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JedisCommands implements RedisCommands<Jedis> {

	@Override
	public Object del(Jedis redis, String... keys) {
		return redis.del(keys);
	}

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
	public Object zadd(Jedis redis, String key, List<ScoredValue<String>> scoredValues) {
		Map<String, Double> scoreMembers = new HashMap<>();
		scoredValues.forEach(v -> scoreMembers.put(v.getValue(), v.getScore()));
		return redis.zadd(key, scoreMembers);
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
	public Object xadd(Jedis redis, String key, List<String> ids, List<Map<String, String>> maps) {
		Object last = null;
		for (int index = 0; index < ids.size(); index++) {
			last = xadd(redis, key, ids.get(index), maps.get(index));
		}
		return last;
	}

	@Override
	public Object set(Jedis redis, String key, String value) {
		return redis.set(key, value);
	}

	@Override
	public Object sadd(Jedis redis, String key, String... members) {
		return redis.sadd(key, members);
	}

	@Override
	public Object rpush(Jedis redis, String key, String... members) {
		return redis.rpush(key, members);
	}

	@Override
	public Object lpush(Jedis redis, String key, String... members) {
		return redis.lpush(key, members);
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
	public Object restore(Jedis redis, String key, byte[] value, long ttl, boolean replace) {
		return redis.restore(key, Math.toIntExact(ttl), value);
	}

	@Override
	public Object ftadd(Jedis redis, String index, Document<String, String> document, AddOptions options) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}

	@Override
	public Object sugadd(Jedis redis, String key, Suggestion<String> suggestion, boolean increment) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}
	
	@Override
	public Object ftsearch(Jedis redis, String index, String query, Object... options) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}
	
	@Override
	public Object ftaggregate(Jedis redis, String index, String query, Cursor cursor, Object... options) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}
	
	@Override
	public Object ftaggregate(Jedis redis, String index, String query, Object... options) {
		throw new UnsupportedOperationException("Jedis not supported with RediSearch");
	}

}
