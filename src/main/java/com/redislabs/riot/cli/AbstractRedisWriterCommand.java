package com.redislabs.riot.cli;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.redis.RedisConverter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.JedisWriter;
import com.redislabs.riot.redis.writer.LettuceAsyncWriter;

import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import picocli.CommandLine.ArgGroup;
import redis.clients.jedis.JedisPool;

public abstract class AbstractRedisWriterCommand<C extends RedisAsyncCommands<String, String>>
		extends AbstractWriterCommand {

	@ArgGroup(exclusive = false, heading = "Redis connection%n")
	protected RedisConnectionOptions redis = new RedisConnectionOptions();
	@ArgGroup(exclusive = false, heading = "Redis keyspace%n")
	private RedisKeyOptions key = new RedisKeyOptions();

	@Override
	protected final ItemWriter<Map<String, Object>> writer() {
		AbstractRedisItemWriter<C> itemWriter = redisItemWriter();
		itemWriter.setConverter(redisConverter());
		switch (redis.getDriver()) {
		case Lettuce:
			return lettuceWriter(itemWriter);
		default:
			return jedisWriter(redis.jedisPool(), itemWriter);
		}
	}

	protected ItemWriter<Map<String, Object>> jedisWriter(JedisPool pool, AbstractRedisItemWriter<C> itemWriter) {
		return new JedisWriter(pool, itemWriter);
	}

	protected LettuceAsyncWriter<? extends StatefulRedisConnection<String, String>, C> lettuceWriter(
			AbstractRedisItemWriter<C> itemWriter) {
		return new LettuceAsyncWriter<StatefulRedisConnection<String, String>, C>(redis.lettucePool(), itemWriter);
	}

	protected abstract AbstractRedisItemWriter<C> redisItemWriter();

	protected RedisConverter redisConverter() {
		return new RedisConverter(key.getSeparator(), key.getSpace(), key.getNames());
	}

	protected String keyspaceDescription() {
		if (key.getSpace() == null) {
			return keysDescription();
		}
		if (key.getNames().length > 0) {
			return key.getSpace() + key.getSeparator() + keysDescription();
		}
		return key.getSpace();
	}

	private String keysDescription() {
		return String.join(key.getSeparator(), key.getNames());
	}

}
