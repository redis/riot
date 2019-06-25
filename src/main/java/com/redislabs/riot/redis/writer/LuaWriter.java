package com.redislabs.riot.redis.writer;

import java.util.Arrays;
import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.async.RedisAsyncCommands;
import lombok.Setter;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

public class LuaWriter extends AbstractRedisDataStructureItemWriter {

	private String sha;
	@Setter
	private String[] keys;
	@Setter
	private String[] args;
	@Setter
	private ScriptOutputType outputType;

	public LuaWriter(String sha) {
		this.sha = sha;
	}

	@Override
	protected Response<Object> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.evalsha(sha, Arrays.asList(keys(item)), Arrays.asList(args(item)));
	}

	private String[] keys(Map<String, Object> item) {
		return array(keys, item);
	}

	private String[] args(Map<String, Object> item) {
		return array(args, item);
	}

	private String[] array(String[] fields, Map<String, Object> item) {
		String[] array = new String[fields.length];
		for (int index = 0; index < fields.length; index++) {
			array[index] = convert(item.get(fields[index]), String.class);
		}
		return array;
	}

	@Override
	protected RedisFuture<?> write(RedisAsyncCommands<String, String> commands, String key, Map<String, Object> item) {
		return commands.evalsha(sha, outputType, keys(item), args(item));
	}

}