package com.redis.riot.cli.common;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.RedisItemReader.BaseBuilder;
import com.redis.spring.batch.common.KeyPredicateFactory;
import com.redis.spring.batch.common.PredicateItemProcessor;

import io.lettuce.core.codec.RedisCodec;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	protected RedisReaderOptions readerOptions = new RedisReaderOptions();

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	protected <K, V> RedisItemReader.Builder<K, V> reader(CommandContext context, RedisCodec<K, V> codec) {
		return configure(RedisItemReader.client(context.getRedisClient(), codec), codec);
	}

	protected <K, V, B extends BaseBuilder<K, V, B>> B configure(B builder, RedisCodec<K, V> codec) {
		builder.jobRepository(getJobRepository());
		builder.options(readerOptions.readerOptions());
		builder.keyProcessor(keyProcessor(codec));
		return builder;
	}

	private <K> ItemProcessor<K, K> keyProcessor(RedisCodec<K, ?> codec) {
		if (readerOptions.getKeySlots().isEmpty() && readerOptions.getKeyExcludes().isEmpty()
				&& readerOptions.getKeyIncludes().isEmpty()) {
			return null;
		}
		KeyPredicateFactory predicateFactory = KeyPredicateFactory.create();
		predicateFactory.slotRange(readerOptions.getKeySlots());
		predicateFactory.include(readerOptions.getKeyIncludes());
		predicateFactory.exclude(readerOptions.getKeyExcludes());
		return new PredicateItemProcessor<>(predicateFactory.build(codec));
	}

	@Override
	public String toString() {
		return "AbstractExportCommand [readerOptions=" + readerOptions + ", " + super.toString() + "]";
	}

}
