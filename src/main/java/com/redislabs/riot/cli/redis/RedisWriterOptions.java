package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.redis.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.JedisClusterWriter;
import com.redislabs.riot.batch.redis.JedisPipelineWriter;
import com.redislabs.riot.batch.redis.LettuceAsyncItemWriter;
import com.redislabs.riot.batch.redis.LettuceConnector;
import com.redislabs.riot.batch.redis.RedisWriter;
import com.redislabs.riot.batch.redis.map.AbstractMapWriter;
import com.redislabs.riot.batch.redis.map.JedisClusterCommands;
import com.redislabs.riot.batch.redis.map.JedisPipelineCommands;
import com.redislabs.riot.batch.redis.map.LettuceAsyncCommands;
import com.redislabs.riot.batch.redis.map.RedisCommands;
import com.redislabs.riot.cli.RedisCommand;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
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
	private RedisCommandOptions redisWriterOptions = new RedisCommandOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch command options%n", order = 30)
	private RediSearchCommandOptions rediSearchWriterOptions = new RediSearchCommandOptions();

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ItemWriter<Map<String, Object>> writer(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterWriter<>(redis.jedisCluster(), mapWriter(redis));
			}
			return new JedisPipelineWriter<>(redis.jedisPool(), mapWriter(redis));
		}
		return new LettuceAsyncItemWriter(lettuceConnector(redis), mapWriter(redis), redis.getCommandTimeout());
	}

	@SuppressWarnings("rawtypes")
	private LettuceConnector lettuceConnector(RedisOptions redis) {
		if (isRediSearch()) {
			RediSearchClient client = redis.lettuSearchClient();
			return new LettuceConnector<String, String, StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRediSearchConnection::async);
		}
		if (redis.isCluster()) {
			RedisClusterClient client = redis.lettuceClusterClient();
			return new LettuceConnector<String, String, StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), StatefulRedisClusterConnection::async);
		}
		RedisClient client = redis.lettuceClient();
		return new LettuceConnector<String, String, StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
				client, client::getResources, redis.pool(client::connect), StatefulRedisConnection::async);

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
		return new LettuceAsyncCommands();
	}

	private <R> AbstractRedisWriter<R, Map<String, Object>> writer() {
		if (isRediSearch()) {
			return rediSearchWriterOptions.writer(command);
		}
		return redisWriterOptions.writer(command);
	}

	private boolean isRediSearch() {
		return command == RedisCommand.ftadd || command == RedisCommand.ftsugadd;
	}

}
