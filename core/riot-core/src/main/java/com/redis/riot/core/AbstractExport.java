package com.redis.riot.core;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.function.DropStreamMessageIdFunction;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.KeyValueOperator;
import com.redis.riot.core.function.LongExpressionFunction;
import com.redis.riot.core.function.StringKeyValueFunction;
import com.redis.riot.core.function.ToStringKeyValueFunction;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.DataType;
import com.redis.spring.batch.common.KeyValue;
import com.redis.spring.batch.reader.AbstractKeyValueItemReader;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractJobRunnable {

	private RedisReaderOptions readerOptions = new RedisReaderOptions();

	protected KeyValueProcessorOptions processorOptions = new KeyValueProcessorOptions();

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	public void setProcessorOptions(KeyValueProcessorOptions options) {
		this.processorOptions = options;
	}

	protected <K> Function<KeyValue<K>, KeyValue<K>> processor(RedisCodec<K, ?> codec, RiotContext context) {
		ToStringKeyValueFunction<K> code = new ToStringKeyValueFunction<>(codec);
		StringKeyValueFunction<K> decode = new StringKeyValueFunction<>(codec);
		UnaryOperator<KeyValue<String>> function = keyValueOperator(context.getEvaluationContext());
		return code.andThen(function).andThen(decode);

	}

	private UnaryOperator<KeyValue<String>> keyValueOperator(EvaluationContext context) {
		KeyValueOperator operator = new KeyValueOperator();
		if (processorOptions.getKeyExpression() != null) {
			operator.setKeyFunction(ExpressionFunction.of(context, processorOptions.getKeyExpression()));
		}
		if (processorOptions.isDropTtl()) {
			operator.setTtlFunction(t -> 0);
		} else {
			if (processorOptions.getTtlExpression() != null) {
				operator.setTtlFunction(new LongExpressionFunction<>(context, processorOptions.getTtlExpression()));
			}
		}
		if (processorOptions.isDropStreamMessageId() && isStruct()) {
			operator.setValueFunction(new DropStreamMessageIdFunction());
		}
		if (processorOptions.getTypeExpression() != null) {
			Function<KeyValue<String>, String> function = ExpressionFunction.of(context,
					processorOptions.getTypeExpression());
			operator.setTypeFunction(function.andThen(DataType::of));
		}
		return operator;
	}

	protected abstract boolean isStruct();

	protected <K, V, R extends RedisItemReader<K, V, ?>> R configureReader(String name, R reader, RedisContext context)
			throws Exception {
		reader.setName(name);
		reader.setJobRepository(jobRepository());
		reader.setTransactionManager(transactionManager());
		reader.setChunkSize(readerOptions.getChunkSize());
		reader.setDatabase(context.getUri().getDatabase());
		reader.setKeyProcessor(keyFilteringProcessor(reader.getCodec()));
		reader.setKeyPattern(readerOptions.getKeyPattern());
		reader.setKeyType(readerOptions.getKeyType());
		reader.setFlushInterval(readerOptions.getFlushInterval());
		reader.setIdleTimeout(readerOptions.getIdleTimeout());
		if (reader instanceof AbstractKeyValueItemReader) {
			AbstractKeyValueItemReader<?, ?> keyValueReader = (AbstractKeyValueItemReader<?, ?>) reader;
			keyValueReader.setMemoryUsageLimit(readerOptions.getMemoryUsageLimit());
			keyValueReader.setMemoryUsageSamples(readerOptions.getMemoryUsageSamples());
			keyValueReader.setPoolSize(readerOptions.getPoolSize());
		}
		reader.setKeyspaceNotificationQueueCapacity(readerOptions.getNotificationQueueCapacity());
		reader.setPollTimeout(readerOptions.getPollTimeout());
		reader.setQueueCapacity(readerOptions.getQueueCapacity());
		reader.setReadFrom(readerOptions.getReadFrom());
		reader.setScanCount(readerOptions.getScanCount());
		reader.setThreads(readerOptions.getThreads());
		return reader;
	}

	public <K> ItemProcessor<K, K> keyFilteringProcessor(RedisCodec<K, ?> codec) {
		Predicate<K> predicate = RiotUtils.keyFilterPredicate(codec, readerOptions.getKeyFilterOptions());
		if (predicate == null) {
			return new PassThroughItemProcessor<>();
		}
		return new PredicateItemProcessor<>(predicate);
	}

}
