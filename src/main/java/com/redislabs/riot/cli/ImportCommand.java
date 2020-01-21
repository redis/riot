package com.redislabs.riot.cli;

import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.redis.JedisClusterCommands;
import com.redislabs.riot.redis.JedisPipelineCommands;
import com.redislabs.riot.redis.LettuceAsyncCommands;
import com.redislabs.riot.redis.LettuceReactiveCommands;
import com.redislabs.riot.redis.LettuceSyncCommands;
import com.redislabs.riot.redis.RedisCommands;
import com.redislabs.riot.redis.writer.AbstractLettuceItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.AbstractRedisWriter;
import com.redislabs.riot.redis.writer.AsyncLettuceItemWriter;
import com.redislabs.riot.redis.writer.ClusterJedisWriter;
import com.redislabs.riot.redis.writer.PipelineJedisWriter;
import com.redislabs.riot.redis.writer.ReactiveLettuceItemWriter;
import com.redislabs.riot.redis.writer.SyncLettuceItemWriter;
import com.redislabs.riot.redis.writer.map.AbstractRediSearchWriter;

import io.lettuce.core.AbstractRedisClient;
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

@Slf4j
@SuppressWarnings({ "unchecked", "rawtypes" })
@Command
public abstract class ImportCommand<I, O> extends TransferCommand<I, O> {

	public void execute(AbstractRedisWriter redisWriter) {
		boolean isRediSearch = redisWriter instanceof AbstractRediSearchWriter;
		redisWriter.commands(redisCommands(redisOptions()));
		AbstractRedisItemWriter writer = itemWriter(redisOptions(), isRediSearch);
		writer.writer(redisWriter);
		ItemReader<I> reader;
		ItemProcessor<I, O> processor;
		try {
			reader = reader();
			processor = processor();
		} catch (Exception e) {
			log.error("Could not initialize import", e);
			return;
		}
		execute(transfer(reader, processor, writer));
	}

	protected abstract ItemReader<I> reader() throws Exception;

	protected ItemProcessor<I, O> processor() throws Exception {
		return null;
	}

	@Override
	protected String taskName() {
		return "Importing";
	}

	private AbstractRedisItemWriter<O> itemWriter(RedisOptions redis, boolean isRediSearch) {
		if (redis.isJedis()) {
			if (redis.cluster()) {
				return new ClusterJedisWriter<O>(redis.jedisCluster());
			}
			return new PipelineJedisWriter<O>(redis.jedisPool());
		}
		AbstractLettuceItemWriter writer = lettuceItemWriter(redis);
		writer.api(lettuceApi(redis, isRediSearch));
		AbstractRedisClient client = lettuceClient(redis, isRediSearch);
		writer.pool(redis.pool(lettuceConnectionSupplier(client)));
		return writer;
	}

	private Supplier lettuceConnectionSupplier(AbstractRedisClient client) {
		if (client instanceof RediSearchClient) {
			return ((RediSearchClient) client)::connect;
		}
		if (client instanceof RedisClusterClient) {
			return ((RedisClusterClient) client)::connect;
		}
		return ((RedisClient) client)::connect;
	}

//	private void closeLettuce() {
//		log.debug("Closing Lettuce pool");
//		pool.close();
//		log.debug("Shutting down Lettuce client");
//		client.shutdown();
//		log.debug("Shutting down Lettuce client resources");
//		resources.get().shutdown();
//	}

//	private Supplier lettuceResources(AbstractRedisClient client) {
//		if (client instanceof RediSearchClient) {
//			return ((RediSearchClient) client)::getResources;
//		}
//		if (client instanceof RedisClusterClient) {
//			return ((RedisClusterClient) client)::getResources;
//		}
//		return ((RedisClient) client)::getResources;
//	}

	private AbstractLettuceItemWriter lettuceItemWriter(RedisOptions redis) {
		switch (redis.lettuce().api()) {
		case Reactive:
			return new ReactiveLettuceItemWriter<>();
		case Sync:
			return new SyncLettuceItemWriter<>();
		default:
			AsyncLettuceItemWriter lettuceAsyncItemWriter = new AsyncLettuceItemWriter();
			lettuceAsyncItemWriter.timeout(redis.lettuce().commandTimeout());
			return lettuceAsyncItemWriter;
		}
	}

	private Function lettuceApi(RedisOptions redis, boolean isRediSearch) {
		if (isRediSearch) {
			switch (redis.lettuce().api()) {
			case Sync:
				return (Function<StatefulRediSearchConnection, RediSearchCommands>) StatefulRediSearchConnection::sync;
			case Reactive:
				return (Function<StatefulRediSearchConnection, RediSearchReactiveCommands>) StatefulRediSearchConnection::reactive;
			default:
				return (Function<StatefulRediSearchConnection, RediSearchAsyncCommands>) StatefulRediSearchConnection::async;
			}
		}
		if (redis.cluster()) {
			switch (redis.lettuce().api()) {
			case Sync:
				return (Function<StatefulRedisClusterConnection, RedisClusterCommands>) StatefulRedisClusterConnection::sync;
			case Reactive:
				return (Function<StatefulRedisClusterConnection, RedisClusterReactiveCommands>) StatefulRedisClusterConnection::reactive;
			default:
				return (Function<StatefulRedisClusterConnection, RedisClusterAsyncCommands>) StatefulRedisClusterConnection::async;
			}
		}
		switch (redis.lettuce().api()) {
		case Sync:
			return (Function<StatefulRedisConnection, io.lettuce.core.api.sync.RedisCommands>) StatefulRedisConnection::sync;
		case Reactive:
			return (Function<StatefulRedisConnection, RedisReactiveCommands>) StatefulRedisConnection::reactive;
		default:
			return (Function<StatefulRedisConnection, RedisAsyncCommands>) StatefulRedisConnection::async;
		}
	}

	protected AbstractRedisClient lettuceClient(RedisOptions redis, boolean rediSearch) {
		if (rediSearch) {
			return redis.lettuSearchClient();
		}
		return redis.lettuceClient();
	}

	private RedisCommands redisCommands(RedisOptions redis) {
		if (redis.isJedis()) {
			if (redis.cluster()) {
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
