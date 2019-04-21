package com.redislabs.riot.redis.writer;

import java.util.Map;

import com.redislabs.lettusearch.search.AddOptions;

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
	public Object xadd(Object redis, String key, String id, Map<String, Object> item, long maxlen,
			boolean approximateTrimming) {
		return ((Pipeline) redis).xadd(key, new StreamEntryID(id), stringMap(item), maxlen, approximateTrimming);
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, Object> item) {
		return ((Pipeline) redis).xadd(key, new StreamEntryID(), stringMap(item));
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, Object> item, long maxlen, boolean approximateTrimming) {
		return ((Pipeline) redis).xadd(key, new StreamEntryID(), stringMap(item), maxlen, approximateTrimming);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, Object> item) {
		return ((Pipeline) redis).xadd(key, new StreamEntryID(id), stringMap(item));
	}

	@Override
	public Object set(Object redis, String key, String string) {
		return ((Pipeline) redis).set(key, string);
	}

	@Override
	public Object zadd(Object redis, String key, double score, String member) {
		return ((Pipeline) redis).zadd(key, score, member);
	}

	@Override
	public Object ftadd(Object redis, String index, String docId, double score, Map<String, Object> item,
			AddOptions options, String payload) {
		throw new RuntimeException("FT.ADD Not supported in JedisCommands");
	}

	@Override
	public Object sugadd(Object redis, String index, String string, double score, boolean increment, String payload) {
		throw new RuntimeException("SUG.ADD Not supported in JedisCommands");
	}

}
