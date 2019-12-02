package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.redis.JedisClusterCommands;
import com.redislabs.riot.batch.redis.JedisPipelineCommands;
import com.redislabs.riot.batch.redis.LettuceAsyncCommands;
import com.redislabs.riot.batch.redis.LettuceConnector;
import com.redislabs.riot.batch.redis.LettuceReactiveCommands;
import com.redislabs.riot.batch.redis.LettuceSyncCommands;
import com.redislabs.riot.batch.redis.RedisCommands;
import com.redislabs.riot.batch.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.batch.redis.writer.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.writer.JedisClusterWriter;
import com.redislabs.riot.batch.redis.writer.JedisPipelineWriter;
import com.redislabs.riot.batch.redis.writer.LettuceAsyncItemWriter;
import com.redislabs.riot.batch.redis.writer.LettuceReactiveItemWriter;
import com.redislabs.riot.batch.redis.writer.LettuceSyncItemWriter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;

@SuppressWarnings({ "unchecked", "rawtypes" })
@Slf4j
@Command
public abstract class ImportCommand<I, O> extends TransferCommand {

	public void execute(AbstractRedisWriter redisWriter) {
		ItemReader<I> reader;
		try {
			reader = reader();
		} catch (Exception e) {
			log.error("Could not initialize reader", e);
			return;
		}
		ItemProcessor<I, O> processor;
		try {
			processor = processor();
		} catch (Exception e) {
			log.error("Could not initialize processor", e);
			return;
		}
		RedisOptions redis = redisOptions();
		AbstractRedisItemWriter writer = writer(redis, redisWriter.isRediSearch());
		redisWriter.commands(redisCommands(redis));
		writer.writer(redisWriter);
		execute(reader, processor, writer);
	}

	protected abstract ItemReader<I> reader() throws Exception;

	protected ItemProcessor<I, O> processor() throws Exception {
		return null;
	}

	private AbstractRedisItemWriter<?, O> writer(RedisOptions redis, boolean rediSearch) {
		if (rediSearch) {
			return rediSearchWriter(redis);
		}
		return redisWriter(redis);
	}

	private AbstractRedisItemWriter<?, O> rediSearchWriter(RedisOptions redis) {
		switch (redis.lettuce().api()) {
		case Reactive:
			return new LettuceReactiveItemWriter<>(lettuSearchConnector(redis));
		case Sync:
			return new LettuceSyncItemWriter<>(lettuSearchConnector(redis));
		default:
			return new LettuceAsyncItemWriter(lettuSearchConnector(redis), redis.getCommandTimeout());
		}
	}

	private AbstractRedisItemWriter<?, O> redisWriter(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterWriter<O>(redis.jedisCluster());
			}
			return new JedisPipelineWriter<O>(redis.jedisPool());
		}
		switch (redis.lettuce().api()) {
		case Reactive:
			return new LettuceReactiveItemWriter<>(lettuceConnector(redis));
		case Sync:
			return new LettuceSyncItemWriter<>(lettuceConnector(redis));
		default:
			return new LettuceAsyncItemWriter(lettuceConnector(redis), redis.getCommandTimeout());
		}
	}

	private LettuceConnector lettuSearchConnector(RedisOptions redis) {
		RediSearchClient client = redis.lettuSearchClient();
		switch (redis.lettuce().api()) {
		case Sync:
			return new LettuceConnector<StatefulRediSearchConnection<String, String>, RediSearchCommands<String, String>>(
					client, client::connect, client::getResources, redis.pool(client::connect),
					StatefulRediSearchConnection::sync);
		case Reactive:
			return new LettuceConnector<StatefulRediSearchConnection<String, String>, RediSearchReactiveCommands<String, String>>(
					client, client::connect, client::getResources, redis.pool(client::connect),
					StatefulRediSearchConnection::reactive);
		default:
			return new LettuceConnector<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
					client, client::connect, client::getResources, redis.pool(client::connect),
					StatefulRediSearchConnection::async);
		}
	}

	protected LettuceConnector lettuceConnector(RedisOptions redis) {
		if (redis.isCluster()) {
			RedisClusterClient client = redis.lettuceClusterClient();
			switch (redis.lettuce().api()) {
			case Sync:
				return new LettuceConnector<StatefulRedisClusterConnection<String, String>, RedisClusterCommands<String, String>>(
						client, client::connect, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::sync);
			case Reactive:
				return new LettuceConnector<StatefulRedisClusterConnection<String, String>, RedisClusterReactiveCommands<String, String>>(
						client, client::connect, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::reactive);
			default:
				return new LettuceConnector<StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>>(
						client, client::connect, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::async);
			}
		}
		RedisClient client = redis.lettuceClient();
		switch (redis.lettuce().api()) {
		case Sync:
			return new LettuceConnector<StatefulRedisConnection<String, String>, io.lettuce.core.api.sync.RedisCommands<String, String>>(
					client, client::connect, client::getResources, redis.pool(client::connect),
					StatefulRedisConnection::sync);
		case Reactive:
			return new LettuceConnector<StatefulRedisConnection<String, String>, RedisReactiveCommands<String, String>>(
					client, client::connect, client::getResources, redis.pool(client::connect),
					StatefulRedisConnection::reactive);
		default:
			return new LettuceConnector<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
					client, client::connect, client::getResources, redis.pool(client::connect),
					StatefulRedisConnection::async);
		}
	}

	private RedisCommands redisCommands(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterCommands();
			}
			return new JedisPipelineCommands();
		}
		switch (redis.lettuce().api()) {
		case Reactive:
			return new LettuceReactiveCommands();
		case Sync:
			return new LettuceSyncCommands();
		default:
			return new LettuceAsyncCommands();
		}
	}
}
