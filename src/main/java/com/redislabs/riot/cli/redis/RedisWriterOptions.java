package com.redislabs.riot.cli.redis;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.riot.batch.redis.JedisClusterItemWriter;
import com.redislabs.riot.batch.redis.JedisItemWriter;
import com.redislabs.riot.batch.redis.LettuceItemWriter;
import com.redislabs.riot.batch.redis.writer.AbstractFlatMapWriter;
import com.redislabs.riot.batch.redis.writer.RedisMapWriter;
import com.redislabs.riot.batch.redisearch.writer.AbstractLettuSearchMapWriter;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisClusterAsyncCommands;
import lombok.Data;
import picocli.CommandLine.ArgGroup;

public @Data class RedisWriterOptions {

	@ArgGroup(exclusive = false, heading = "Redis key options%n", order = 10)
	private KeyOptions keyOptions = new KeyOptions();
	@ArgGroup(exclusive = false, heading = "Redis command options%n", order = 20)
	private RedisCommandOptions redisCommandOptions = new RedisCommandOptions();
	@ArgGroup(exclusive = false, heading = "RediSearch command options%n", order = 30)
	private RediSearchCommandOptions rediSearchCommandOptions = new RediSearchCommandOptions();

	public ItemWriter<Map<String, Object>> writer(RedisConnectionOptions redis) {
		RedisMapWriter mapWriter = rediSearchCommandOptions.isSet() ? rediSearchCommandOptions.writer() : redisCommandOptions.writer();
		if (mapWriter instanceof AbstractFlatMapWriter) {
			((AbstractFlatMapWriter) mapWriter).setConverter(keyOptions.converter());
		}
		if (mapWriter instanceof AbstractLettuSearchMapWriter) {
			RediSearchClient client = redis.lettuSearchClient();
			return new LettuceItemWriter<StatefulRediSearchConnection<String, String>, RediSearchAsyncCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), mapWriter,
					StatefulRediSearchConnection::async, redis.getCommandTimeout());
		}
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new JedisClusterItemWriter(redis.jedisCluster(), mapWriter);
			}
			return new JedisItemWriter(redis.jedisPool(), mapWriter);
		}
		if (redis.isCluster()) {
			RedisClusterClient client = redis.lettuceClusterClient();
			return new LettuceItemWriter<StatefulRedisClusterConnection<String, String>, RedisClusterAsyncCommands<String, String>>(
					client, client::getResources, redis.pool(client::connect), mapWriter,
					StatefulRedisClusterConnection::async, redis.getCommandTimeout());
		}
		RedisClient client = redis.lettuceClient();
		return new LettuceItemWriter<StatefulRedisConnection<String, String>, RedisAsyncCommands<String, String>>(
				client, client::getResources, redis.pool(client::connect), mapWriter, StatefulRedisConnection::async,
				redis.getCommandTimeout());
	}

}
