package com.redislabs.riot.cli.redis.commands;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.HelpCommand;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.redis.AbstractRedisItemWriter;
import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.JedisClusterCommands;
import com.redislabs.riot.batch.redis.JedisClusterWriter;
import com.redislabs.riot.batch.redis.JedisPipelineCommands;
import com.redislabs.riot.batch.redis.JedisPipelineWriter;
import com.redislabs.riot.batch.redis.LettuceAsyncCommands;
import com.redislabs.riot.batch.redis.LettuceAsyncItemWriter;
import com.redislabs.riot.batch.redis.LettuceConnector;
import com.redislabs.riot.batch.redis.LettuceReactiveCommands;
import com.redislabs.riot.batch.redis.LettuceReactiveItemWriter;
import com.redislabs.riot.batch.redis.LettuceSyncCommands;
import com.redislabs.riot.batch.redis.LettuceSyncItemWriter;
import com.redislabs.riot.batch.redis.RedisCommands;
import com.redislabs.riot.cli.ImportCommand;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@SuppressWarnings({ "unchecked", "rawtypes" })
@Command
public abstract class AbstractRedisCommand extends HelpCommand implements Runnable {

	@ParentCommand
	private ImportCommand parent;
	@Spec
	private CommandSpec spec;

	@Override
	public void run() {
		RedisOptions redis = parent.redisOptions();
		AbstractRedisItemWriter itemWriter = itemWriter(redis);
		AbstractRedisWriter writer = writer();
		writer.commands(redisCommands(redis));
		itemWriter.writer(writer);
		parent.execute(itemWriter);
	}

	protected abstract AbstractRedisWriter writer();

	private AbstractRedisItemWriter itemWriter(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterWriter<>(redis.jedisCluster());
			}
			return new JedisPipelineWriter<>(redis.jedisPool());
		}
		switch (redis.getLettuce().getApi()) {
		case Reactive:
			return new LettuceReactiveItemWriter<>(lettuceConnector(redis));
		case Sync:
			return new LettuceSyncItemWriter<>(lettuceConnector(redis));
		default:
			return new LettuceAsyncItemWriter(lettuceConnector(redis), redis.getCommandTimeout());
		}
	}

//	@SuppressWarnings("unchecked")
//	private <R> RedisWriter<R, Map<String, Object>> mapWriter(RedisOptions redis) {
//		AbstractRedisWriter<R, Map<String, Object>> writer = command.writer();
//		writer.commands(redisCommands(redis));
//		if (writer instanceof AbstractMapWriter) {
//			AbstractMapWriter<R> mapWriter = (AbstractMapWriter<R>) writer;
//			mapWriter.converter(keyOptions.converter());
//		}
//		return writer;
//	}

	private LettuceConnector lettuceConnector(RedisOptions redis) {
		if (isRediSearch()) {
			RediSearchClient client = redis.lettuSearchClient();
			switch (redis.getLettuce().getApi()) {
			case Sync:
				return new LettuceConnector<String, String, StatefulRediSearchConnection<String, String>, RediSearchCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect), StatefulRediSearchConnection::sync);
			case Reactive:
				return new LettuceConnector<String, String, StatefulRediSearchConnection<String, String>, RediSearchReactiveCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect),
						StatefulRediSearchConnection::reactive);
			default:
				return new LettuceConnector<String, String, StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect), StatefulRediSearchConnection::async);
			}
		}
		if (redis.isCluster()) {
			RedisClusterClient client = redis.lettuceClusterClient();
			switch (redis.getLettuce().getApi()) {
			case Sync:
				return new LettuceConnector<String, String, StatefulRedisClusterConnection<String, String>, RedisClusterCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::sync);
			case Reactive:
				return new LettuceConnector<String, String, StatefulRedisClusterConnection<String, String>, RedisClusterReactiveCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::reactive);
			default:
				return new LettuceConnector<String, String, StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::async);
			}
		}
		RedisClient client = redis.lettuceClient();
		switch (redis.getLettuce().getApi()) {
		case Sync:
			return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, io.lettuce.core.api.sync.RedisCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::sync);
		case Reactive:
			return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, RedisReactiveCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::reactive);
		default:
			return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::async);
		}
	}

	protected boolean isRediSearch() {
		return false;
	}

	private RedisCommands redisCommands(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterCommands();
			}
			return new JedisPipelineCommands();
		}
		switch (redis.getLettuce().getApi()) {
		case Reactive:
			return new LettuceReactiveCommands();
		case Sync:
			return new LettuceSyncCommands();
		default:
			return new LettuceAsyncCommands();
		}
	}

}
