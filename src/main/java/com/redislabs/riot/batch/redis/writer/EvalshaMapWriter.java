package com.redislabs.riot.batch.redis.writer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisScriptingAsyncCommands;
import lombok.Setter;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class EvalshaMapWriter extends AbstractRedisFlatMapWriter {

	@Setter
	private String sha;
	@Setter
	private List<String> keys;
	@Setter
	private List<String> args;
	@Setter
	private ScriptOutputType outputType;

	@Override
	protected Response<Object> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.evalsha(sha, Arrays.asList(keys(item)), Arrays.asList(args(item)));
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.evalsha(sha, Arrays.asList(keys(item)), Arrays.asList(args(item)));
	}

	private String[] keys(Map<String, Object> item) {
		return array(keys, item);
	}

	private String[] args(Map<String, Object> item) {
		return array(args, item);
	}

	private String[] array(List<String> fields, Map<String, Object> item) {
		String[] array = new String[fields.size()];
		for (int index = 0; index < fields.size(); index++) {
			array[index] = convert(item.get(fields.get(index)), String.class);
		}
		return array;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected RedisFuture<?> write(Object commands, String key, Map<String, Object> item) {
		return ((RedisScriptingAsyncCommands<String, String>) commands).evalsha(sha, outputType, keys(item),
				args(item));
	}

}