package com.redislabs.riot.redis.writer;

import java.util.Map;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.async.RedisStreamAsyncCommands;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.StreamEntryID;

public class XaddIdItemWriter extends RedisItemWriter<RedisStreamAsyncCommands<String, String>> {

	private String idField;

	public void setIdField(String idField) {
		this.idField = idField;
	}

	private String id(Map<String, Object> item) {
		return convert(item.remove(idField), String.class);
	}

	@Override
	protected Response<StreamEntryID> write(Pipeline pipeline, String key, Map<String, Object> item) {
		return pipeline.xadd(key, new StreamEntryID(id(item)), stringMap(item));
	}

	@Override
	protected void write(JedisCluster cluster, String key, Map<String, Object> item) {
		cluster.xadd(key, new StreamEntryID(id(item)), stringMap(item));
	}

	@Override
	protected RedisFuture<?> write(RedisStreamAsyncCommands<String, String> commands, String key,
			Map<String, Object> item) {
		return commands.xadd(key, xAddArgs(item), stringMap(item));
	}

	private XAddArgs xAddArgs(Map<String, Object> item) {
		return new XAddArgs().id(id(item));
	}

}