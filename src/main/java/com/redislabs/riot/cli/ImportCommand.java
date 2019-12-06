package com.redislabs.riot.cli;

import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.lettusearch.RediSearchAsyncCommands;
import com.redislabs.lettusearch.RediSearchClient;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchReactiveCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.riot.batch.Transfer;
import com.redislabs.riot.batch.TransferContext;
import com.redislabs.riot.batch.redis.JedisClusterCommands;
import com.redislabs.riot.batch.redis.JedisPipelineCommands;
import com.redislabs.riot.batch.redis.LettuceAsyncCommands;
import com.redislabs.riot.batch.redis.LettuceReactiveCommands;
import com.redislabs.riot.batch.redis.LettuceSyncCommands;
import com.redislabs.riot.batch.redis.RedisCommands;
import com.redislabs.riot.batch.redis.writer.AbstractLettuceItemWriter;
import com.redislabs.riot.batch.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.batch.redis.writer.AbstractRedisWriter;
import com.redislabs.riot.batch.redis.writer.AsyncLettuceItemWriter;
import com.redislabs.riot.batch.redis.writer.ClusterJedisWriter;
import com.redislabs.riot.batch.redis.writer.PipelineJedisWriter;
import com.redislabs.riot.batch.redis.writer.ReactiveLettuceItemWriter;
import com.redislabs.riot.batch.redis.writer.SyncLettuceItemWriter;
import com.redislabs.riot.batch.redis.writer.map.AbstractRediSearchWriter;

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
import picocli.CommandLine.Command;

@SuppressWarnings({ "unchecked", "rawtypes" })
@Command
public abstract class ImportCommand<I, O> extends TransferCommand {

	public void execute(String unitName, AbstractRedisWriter redisWriter) {
		boolean isRediSearch = redisWriter instanceof AbstractRediSearchWriter;
		redisWriter.commands(redisCommands(redisOptions()));
		AbstractRedisItemWriter writer = itemWriter(redisOptions(), isRediSearch);
		writer.writer(redisWriter);
		execute(new Transfer<I, O>() {
			@Override
			public ItemReader<I> reader(TransferContext context) throws Exception {
				return ImportCommand.this.reader(context);
			}

			@Override
			public ItemProcessor<I, O> processor(TransferContext context) throws Exception {
				return ImportCommand.this.processor();
			}

			@Override
			public ItemWriter<O> writer(TransferContext context) throws Exception {
				return writer;
			}

			@Override
			public String unitName() {
				return unitName;
			}

			@Override
			public String taskName() {
				return ImportCommand.this.taskName();
			}

		});
	}

	private AbstractRedisItemWriter<?, O> itemWriter(RedisOptions redis, boolean isRediSearch) {
		if (redis.isJedis()) {
			if (redis.isCluster()) {
				return new ClusterJedisWriter<O>(redis.jedisCluster());
			}
			return new PipelineJedisWriter<O>(redis.jedisPool());
		}
		AbstractLettuceItemWriter writer = lettuceItemWriter(redis);
		writer.api(lettuceApi(redis, isRediSearch));
		AbstractRedisClient client = lettuceClient(redis, isRediSearch);
		writer.client(client);
		writer.pool(redis.pool(lettuceConnectionSupplier(client)));
		writer.resources(lettuceResources(client));
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

	private Supplier lettuceResources(AbstractRedisClient client) {
		if (client instanceof RediSearchClient) {
			return ((RediSearchClient) client)::getResources;
		}
		if (client instanceof RedisClusterClient) {
			return ((RedisClusterClient) client)::getResources;
		}
		return ((RedisClient) client)::getResources;
	}

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

	protected abstract String taskName();

	protected abstract ItemReader<I> reader(TransferContext context) throws Exception;

	protected ItemProcessor<I, O> processor() throws Exception {
		return null;
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
		if (redis.isCluster()) {
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
		if (redis.isCluster()) {
			return redis.lettuceClusterClient();
		}
		return redis.lettuceClient();
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
