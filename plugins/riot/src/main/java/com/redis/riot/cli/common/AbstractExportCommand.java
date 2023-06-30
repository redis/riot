package com.redis.riot.cli.common;

import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;

import com.redis.spring.batch.RedisItemReader.BaseScanBuilder;
import com.redis.spring.batch.RedisItemReader.ScanBuilder;
import com.redis.spring.batch.common.KeyPredicateFactory;
import com.redis.spring.batch.common.PoolOptions;
import com.redis.spring.batch.common.PredicateItemProcessor;
import com.redis.spring.batch.common.Utils;
import com.redis.spring.batch.reader.QueueOptions;
import com.redis.spring.batch.reader.ScanSizeEstimator;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import picocli.CommandLine.ArgGroup;

public abstract class AbstractExportCommand extends AbstractCommand {

	@ArgGroup(exclusive = false, heading = "Redis reader options%n")
	private RedisReaderOptions readerOptions = new RedisReaderOptions();

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	protected ScanBuilder scanBuilder(CommandContext context) {
		ScanBuilder scanBuilder = new ScanBuilder(context.getRedisClient());
		configureScanBuilder(scanBuilder);
		return scanBuilder;
	}

	protected void configureScanBuilder(BaseScanBuilder<?> builder) {
		builder.jobRepository(getJobRepository());
		builder.options(readerOptions.readerOptions());
		builder.scanOptions(readerOptions.scanOptions());
	}

	protected StepProgressMonitor monitor(String task, CommandContext context) {
		return monitor(task).withInitialMax(estimator(context));
	}

	protected ScanSizeEstimator estimator(CommandContext context) {
		ScanSizeEstimator estimator = new ScanSizeEstimator(connectionSupplier(context));
		estimator.setOptions(readerOptions.estimatorOptions());
		return estimator;
	}

	private Supplier<StatefulConnection<String, String>> connectionSupplier(CommandContext context) {
		return Utils.connectionSupplier(context.getRedisClient(), readerOptions.readFrom());
	}

	protected PoolOptions poolOptions(RedisReaderOptions options) {
		return PoolOptions.builder().maxTotal(options.getPoolMaxTotal()).build();
	}

	protected QueueOptions queueOptions(RedisReaderOptions options) {
		return QueueOptions.builder().capacity(options.getQueueCapacity()).build();
	}

	protected ItemProcessor<String, String> keyProcessor() {
		return keyProcessor(StringCodec.UTF8);
	}

	protected <K> ItemProcessor<K, K> keyProcessor(RedisCodec<K, ?> codec) {
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

}
