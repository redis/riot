package com.redislabs.riot.redis.writer;

import java.util.Map;

import redis.clients.jedis.Pipeline;
import redis.clients.jedis.StreamEntryID;

public class JedisCommands extends AbstractRedisCommands {

	@Override
	public Object geoadd(Object redis, String key, double longitude, double latitude, String member) {
		return ((Pipeline) redis).geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(Object redis, String key, Map<String, Object> item) {
		return ((Pipeline) redis).hmset(key, stringMap(item));
	}

	@Override
	public Object rpush(Object redis, String key, String member) {
		return ((Pipeline) redis).rpush(key, member);
	}

	@Override
	public Object lpush(Object redis, String key, String member) {
		return ((Pipeline) redis).lpush(key, member);
	}

	@Override
	public Object sadd(Object redis, String key, String member) {
		return ((Pipeline) redis).sadd(key, member);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, Object> item, Long maxlen,
			boolean approximateTrimming) {
		return ((Pipeline) redis).xadd(key, new StreamEntryID(id), stringMap(item), maxlen, approximateTrimming);
	}

	@Override
	public Object set(Object redis, String key, String string) {
		return ((Pipeline) redis).set(key, string);
	}

	@Override
	public Object zadd(Object redis, String key, double score, String member) {
		return ((Pipeline) redis).zadd(key, score, member);
	}

}
