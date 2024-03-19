package com.redis.riot.core;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;

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
import com.redis.spring.batch.step.FlushingStepBuilder;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractJobRunnable {

	private static final String REDIS_VAR = "redis";

	private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();
	private RedisReaderOptions readerOptions = new RedisReaderOptions();
	private ExportProcessorOptions processorOptions = new ExportProcessorOptions();

	protected <K> Function<KeyValue<K>, KeyValue<K>> processor(RedisCodec<K, ?> codec) {
		ToStringKeyValueFunction<K> code = new ToStringKeyValueFunction<>(codec);
		StringKeyValueFunction<K> decode = new StringKeyValueFunction<>(codec);
		UnaryOperator<KeyValue<String>> function = keyValueOperator();
		return code.andThen(function).andThen(decode);
	}

	protected StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext evaluationContext = evaluationContextOptions.evaluationContext();
		evaluationContext.setVariable(REDIS_VAR, getRedisConnection().sync());
		return evaluationContext;
	}

	private UnaryOperator<KeyValue<String>> keyValueOperator() {
		KeyValueOperator operator = new KeyValueOperator();
		StandardEvaluationContext evaluationContext = evaluationContext();
		if (processorOptions.getKeyExpression() != null) {
			operator.setKeyFunction(ExpressionFunction.of(evaluationContext, processorOptions.getKeyExpression()));
		}
		if (processorOptions.isDropTtl()) {
			operator.setTtlFunction(t -> 0);
		} else {
			if (processorOptions.getTtlExpression() != null) {
				operator.setTtlFunction(
						new LongExpressionFunction<>(evaluationContext, processorOptions.getTtlExpression()));
			}
		}
		if (processorOptions.isDropStreamMessageId() && isStruct()) {
			operator.setValueFunction(new DropStreamMessageIdFunction());
		}
		if (processorOptions.getTypeExpression() != null) {
			Function<KeyValue<String>, String> function = ExpressionFunction.of(evaluationContext,
					processorOptions.getTypeExpression());
			operator.setTypeFunction(function.andThen(DataType::of));
		}
		return operator;
	}

	protected abstract boolean isStruct();

	protected <K, V, R extends RedisItemReader<K, V, ?>> R configureReader(String name, R reader) {
		reader.setName(name);
		reader.setJobRepository(getJobRepository());
		reader.setTransactionManager(getTransactionManager());
		reader.setChunkSize(readerOptions.getChunkSize());
		reader.setDatabase(getRedisURI().getDatabase());
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

	protected <I, O> FlushingStepBuilder<I, O> flushingStep(SimpleStepBuilder<I, O> step) {
		return new FlushingStepBuilder<>(step).interval(readerOptions.getFlushInterval())
				.idleTimeout(readerOptions.getIdleTimeout());
	}

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions readerOptions) {
		this.readerOptions = readerOptions;
	}

	public ExportProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public void setProcessorOptions(ExportProcessorOptions options) {
		this.processorOptions = options;
	}

	public void setEvaluationContextOptions(EvaluationContextOptions spelProcessorOptions) {
		this.evaluationContextOptions = spelProcessorOptions;
	}

}
