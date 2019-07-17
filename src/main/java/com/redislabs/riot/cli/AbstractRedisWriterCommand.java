package com.redislabs.riot.cli;

import java.util.Map;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.batch.item.ItemWriter;

import com.redislabs.riot.redis.writer.AbstractRedisItemWriter;
import com.redislabs.riot.redis.writer.JedisWriter;
import com.redislabs.riot.redis.writer.LettuceAsyncWriter;

import io.lettuce.core.api.StatefulRedisConnection;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.ParentCommand;

public abstract class AbstractRedisWriterCommand<S extends StatefulRedisConnection<String, String>>
		extends AbstractCommand {

	@ParentCommand
	private AbstractReaderCommand parent;

	@Mixin
	private RedisConnectionOptions redis = new RedisConnectionOptions();

	public void execute(AbstractRedisItemWriter itemWriter, String description) {
		parent.execute(writer(itemWriter), description);
	}

	private ItemWriter<Map<String, Object>> writer(AbstractRedisItemWriter itemWriter) {
		if (redis.isJedis()) {
			JedisWriter writer = jedisWriter(itemWriter);
			writer.setPool(redis.jedisPool());
			return writer;
		}
		LettuceAsyncWriter<S> writer = lettuceWriter(itemWriter);
		writer.setPool(lettucePool(redis));
		return writer;
	}

	protected abstract GenericObjectPool<S> lettucePool(RedisConnectionOptions redis);

	protected JedisWriter jedisWriter(AbstractRedisItemWriter itemWriter) {
		return new JedisWriter(itemWriter);
	}

	protected LettuceAsyncWriter<S> lettuceWriter(AbstractRedisItemWriter itemWriter) {
		return new LettuceAsyncWriter<S>(itemWriter);
	}

}
