package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.JedisClusterWriter;
import com.redislabs.riot.batch.redis.JedisPipelineWriter;
import com.redislabs.riot.batch.redis.LettuceAsyncItemWriter;
import com.redislabs.riot.batch.redis.LettuceConnector;
import com.redislabs.riot.batch.redis.LettuceReactiveItemWriter;
import com.redislabs.riot.batch.redis.LettuceSyncItemWriter;
import com.redislabs.riot.batch.redis.RedisWriter;
import com.redislabs.riot.batch.redis.map.AbstractMapWriter;
import com.redislabs.riot.batch.redis.map.JedisClusterCommands;
import com.redislabs.riot.batch.redis.map.JedisPipelineCommands;
import com.redislabs.riot.batch.redis.map.LettuceAsyncCommands;
import com.redislabs.riot.batch.redis.map.LettuceReactiveCommands;
import com.redislabs.riot.batch.redis.map.LettuceSyncCommands;
import com.redislabs.riot.batch.redis.map.RedisCommands;
import com.redislabs.riot.cli.RedisCommand;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisClusterReactiveCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public @Data class RedisWriterOptions {

	@Option(names = { "-c",
			"--command" }, description = "Redis command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisCommand command = RedisCommand.hmset;
	@ArgGroup(exclusive = false, heading = "Redis key options%n", order = 10)
	private KeyOptions keyOptions = new KeyOptions();
	@ArgGroup(exclusive = false, heading = "Redis command options%n", order = 20)
	private RedisCommandOptions redisCommandOptions = new RedisCommandOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch command options%n", order = 30)
	private RediSearchCommandOptions rediSearchCommandOptions = new RediSearchCommandOptions();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ItemWriter<Map<String, Object>> writer(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterWriter<>(redis.jedisCluster(), mapWriter(redis));
			}
			return new JedisPipelineWriter<>(redis.jedisPool(), mapWriter(redis));
		}
		switch (redis.getLettuce().getApi()) {
		case reactive:
			return new LettuceReactiveItemWriter<>(lettuceConnector(redis), mapWriter(redis));
		case sync:
			return new LettuceSyncItemWriter<>(lettuceConnector(redis), mapWriter(redis));
		default:
			return new LettuceAsyncItemWriter(lettuceConnector(redis), mapWriter(redis), redis.getCommandTimeout());
		}
	}

	@SuppressWarnings("rawtypes")
	private LettuceConnector lettuceConnector(RedisOptions redis) {
		if (isRediSearch()) {
			RediSearchClient client = redis.lettuSearchClient();
			switch (redis.getLettuce().getApi()) {
			case sync:
				return new LettuceConnector<String, String, StatefulRediSearchConnection<String, String>, RediSearchCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect), StatefulRediSearchConnection::sync);
			case reactive:
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
			case sync:
				return new LettuceConnector<String, String, StatefulRedisClusterConnection<String, String>, RedisClusterCommands<String, String>>(
						client, client::getResources, redis.pool(client::connect),
						StatefulRedisClusterConnection::sync);
			case reactive:
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
		case sync:
			return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, io.lettuce.core.api.sync.RedisCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::sync);
		case reactive:
			return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, RedisReactiveCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::reactive);
		default:
			return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::async);
		}

	}

	@SuppressWarnings("unchecked")
	private <R> RedisWriter<R, Map<String, Object>> mapWriter(RedisOptions redis) {
		AbstractRedisWriter<R, Map<String, Object>> writer = writer();
		writer.commands(redisCommands(redis));
		if (writer instanceof AbstractMapWriter) {
			AbstractMapWriter<R> mapWriter = (AbstractMapWriter<R>) writer;
			mapWriter.converter(keyOptions.converter());
		}
		return writer;
	}

	@SuppressWarnings("rawtypes")
	private RedisCommands redisCommands(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterCommands();
			}
			return new JedisPipelineCommands();
		}
		switch (redis.getLettuce().getApi()) {
		case reactive:
			return new LettuceReactiveCommands();
		case sync:
			return new LettuceSyncCommands();
		default:
			return new LettuceAsyncCommands();
		}
	}

	private <R> AbstractRedisWriter<R, Map<String, Object>> writer() {
		if (isRediSearch()) {
			return rediSearchCommandOptions.writer(command);
		}
		return redisCommandOptions.writer(command);
	}

	private boolean isRediSearch() {
		return command == RedisCommand.ftadd || command == RedisCommand.ftsugadd;
	}

}
