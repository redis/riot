package com.redislabs.riot.redis;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.sync.RedisGeoCommands;
import io.lettuce.core.api.sync.RedisHashCommands;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisListCommands;
import io.lettuce.core.api.sync.RedisScriptingCommands;
import io.lettuce.core.api.sync.RedisSetCommands;
import io.lettuce.core.api.sync.RedisSortedSetCommands;
import io.lettuce.core.api.sync.RedisStreamCommands;
import io.lettuce.core.api.sync.RedisStringCommands;

@SuppressWarnings("unchecked")
public class LettuceSyncCommands implements RedisCommands<Object> {

	@Override
	public Long del(Object redis, String... keys) {
		return ((RedisKeyCommands<String, String>) redis).del(keys);
	}

	@Override
	public Long geoadd(Object redis, String key, double longitude, double latitude, String member) {
		return ((RedisGeoCommands<String, String>) redis).geoadd(key, longitude, latitude, member);
	}

	@Override
	public String hmset(Object redis, String key, Map<String, String> map) {
		return ((RedisHashCommands<String, String>) redis).hmset(key, map);
	}

	@Override
	public String xadd(Object redis, String key, Map<String, String> map) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key, map);
	}

	@Override
	public String xadd(Object redis, String key, String id, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key,
				new XAddArgs().id(id).maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public String xadd(Object redis, String key, String id, Map<String, String> map) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key, new XAddArgs().id(id), map);
	}

	@Override
	public String xadd(Object redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return ((RedisStreamCommands<String, String>) redis).xadd(key,
				new XAddArgs().maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public String xadd(Object redis, String key, List<String> ids, List<Map<String, String>> maps) {
		String last = null;
		for (int index = 0; index < ids.size(); index++) {
			last = xadd(redis, key, ids.get(index), maps.get(index));
		}
		return last;
	}

	@Override
	public Long zadd(Object redis, String key, double score, String member) {
		return ((RedisSortedSetCommands<String, String>) redis).zadd(key, score, member);
	}

	@Override
	public Object zadd(Object redis, String key, List<ScoredValue<String>> scoredValues) {
		return ((RedisSortedSetCommands<String, String>) redis).zadd(key, scoredValues);
	}

	@Override
	public String set(Object redis, String key, String value) {
		return ((RedisStringCommands<String, String>) redis).set(key, value);
	}

	@Override
	public Long sadd(Object redis, String key, String... members) {
		return ((RedisSetCommands<String, String>) redis).sadd(key, members);
	}

	@Override
	public Long rpush(Object redis, String key, String... members) {
		return ((RedisListCommands<String, String>) redis).rpush(key, members);
	}

	@Override
	public Long lpush(Object redis, String key, String... members) {
		return ((RedisListCommands<String, String>) redis).lpush(key, members);
	}

	@Override
	public Boolean expire(Object redis, String key, long timeout) {
		return ((RedisKeyCommands<String, String>) redis).expire(key, timeout);
	}

	@Override
	public Object evalsha(Object redis, String sha, ScriptOutputType type, String[] keys, String[] args) {
		return ((RedisScriptingCommands<String, String>) redis).evalsha(sha, type, keys, args);
	}

	@Override
	public String restore(Object redis, String key, byte[] value, long ttl, boolean replace) {
		RestoreArgs args = new RestoreArgs().ttl(ttl).replace(replace);
		return ((RedisKeyCommands<String, String>) redis).restore(key, value, args);
	}

	@Override
	public String ftadd(Object redis, String index, String docId, double score, Map<String, String> map, String payload,
			AddOptions options) {
		return ((RediSearchCommands<String, String>) redis).add(index, docId, score, map, payload, options);
	}

	@Override
	public Long sugadd(Object redis, String index, String string, double score, boolean increment, String payload) {
		return ((RediSearchCommands<String, String>) redis).sugadd(index, string, score, increment, payload);
	}

	@Override
	public Object ftsearch(Object redis, String index, String query, Object... options) {
		return ((RediSearchCommands<String, String>) redis).search(index, query, options);
	}

	@Override
	public Object ftaggregate(Object redis, String index, String query, Cursor cursor, Object... options) {
		return ((RediSearchCommands<String, String>) redis).aggregate(index, query, cursor, options);
	}

	@Override
	public Object ftaggregate(Object redis, String index, String query, Object... options) {
		return ((RediSearchCommands<String, String>) redis).aggregate(index, query, options);
	}

}
