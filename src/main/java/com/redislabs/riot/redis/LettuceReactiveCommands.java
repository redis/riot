package com.redislabs.riot.redis;

import java.util.List;
import java.util.Map;

import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.lettusearch.search.AddOptions;

import com.redislabs.lettusearch.search.Document;
import com.redislabs.lettusearch.suggest.Suggestion;
import io.lettuce.core.RestoreArgs;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.reactive.RedisGeoReactiveCommands;
import io.lettuce.core.api.reactive.RedisHashReactiveCommands;
import io.lettuce.core.api.reactive.RedisKeyReactiveCommands;
import io.lettuce.core.api.reactive.RedisListReactiveCommands;
import io.lettuce.core.api.reactive.RedisScriptingReactiveCommands;
import io.lettuce.core.api.reactive.RedisSetReactiveCommands;
import io.lettuce.core.api.reactive.RedisSortedSetReactiveCommands;
import io.lettuce.core.api.reactive.RedisStreamReactiveCommands;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;

@SuppressWarnings("unchecked")
public class LettuceReactiveCommands implements RedisCommands<Object> {

	@Override
	public Object del(Object redis, String... keys) {
		return ((RedisKeyReactiveCommands<String, String>) redis).del(keys);
	}

	@Override
	public Object geoadd(Object redis, String key, double longitude, double latitude, String member) {
		return ((RedisGeoReactiveCommands<String, String>) redis).geoadd(key, longitude, latitude, member);
	}

	@Override
	public Object hmset(Object redis, String key, Map<String, String> map) {
		return ((RedisHashReactiveCommands<String, String>) redis).hmset(key, map);
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, String> map) {
		return ((RedisStreamReactiveCommands<String, String>) redis).xadd(key, map);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, String> map, long maxlen,
			boolean approximateTrimming) {
		return ((RedisStreamReactiveCommands<String, String>) redis).xadd(key,
				new XAddArgs().id(id).maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object xadd(Object redis, String key, String id, Map<String, String> map) {
		return ((RedisStreamReactiveCommands<String, String>) redis).xadd(key, new XAddArgs().id(id), map);
	}

	@Override
	public Object xadd(Object redis, String key, Map<String, String> map, long maxlen, boolean approximateTrimming) {
		return ((RedisStreamReactiveCommands<String, String>) redis).xadd(key,
				new XAddArgs().maxlen(maxlen).approximateTrimming(approximateTrimming), map);
	}

	@Override
	public Object xadd(Object redis, String key, List<String> ids, List<Map<String, String>> maps) {
		throw new UnsupportedOperationException("Multi-message xadd not implemented");
	}

	@Override
	public Object zadd(Object redis, String key, double score, String member) {
		return ((RedisSortedSetReactiveCommands<String, String>) redis).zadd(key, score, member);
	}

	@Override
	public Object zadd(Object redis, String key, List<ScoredValue<String>> scoredValues) {
		return ((RedisSortedSetReactiveCommands<String, String>) redis).zadd(key, scoredValues);
	}

	@Override
	public Object set(Object redis, String key, String value) {
		return ((RedisStringReactiveCommands<String, String>) redis).set(key, value);
	}

	@Override
	public Object sadd(Object redis, String key, String... members) {
		return ((RedisSetReactiveCommands<String, String>) redis).sadd(key, members);
	}

	@Override
	public Object rpush(Object redis, String key, String... members) {
		return ((RedisListReactiveCommands<String, String>) redis).rpush(key, members);
	}

	@Override
	public Object lpush(Object redis, String key, String... members) {
		return ((RedisListReactiveCommands<String, String>) redis).lpush(key, members);
	}

	@Override
	public Object expire(Object redis, String key, long timeout) {
		return ((RedisKeyReactiveCommands<String, String>) redis).expire(key, timeout);
	}

	@Override
	public Object evalsha(Object redis, String sha, ScriptOutputType type, String[] keys, String[] args) {
		return ((RedisScriptingReactiveCommands<String, String>) redis).evalsha(sha, type, keys, args);
	}

	@Override
	public Object restore(Object redis, String key, byte[] value, long ttl, boolean replace) {
		RestoreArgs args = new RestoreArgs().ttl(ttl).replace(replace);
		return ((RedisKeyReactiveCommands<String, String>) redis).restore(key, value, args);
	}

	@Override
	public Object ftadd(Object redis, String index, Document<String, String> document, AddOptions options) {
		return ((RediSearchReactiveCommands<String, String>) redis).add(index, document, options);
	}

	@Override
	public Object sugadd(Object redis, String key, Suggestion<String> suggestion, boolean increment) {
		return ((RediSearchReactiveCommands<String, String>) redis).sugadd(key, suggestion, increment);
	}

	@Override
	public Object ftsearch(Object redis, String index, String query, Object... options) {
		return ((RediSearchReactiveCommands<String, String>) redis).search(index, query, options);
	}

	@Override
	public Object ftaggregate(Object redis, String index, String query, Cursor cursor, Object... options) {
		return ((RediSearchReactiveCommands<String, String>) redis).aggregate(index, query, cursor, options);
	}

	@Override
	public Object ftaggregate(Object redis, String index, String query, Object... options) {
		return ((RediSearchReactiveCommands<String, String>) redis).aggregate(index, query, options);
	}

}
