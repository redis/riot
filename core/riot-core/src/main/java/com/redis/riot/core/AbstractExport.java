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
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.KeyValue.Type;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.common.FlushingStepBuilder;
import com.redis.spring.batch.operation.KeyValueRead;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractJobRunnable {

	private static final String REDIS_VAR = "redis";

	private EvaluationContextOptions evaluationContextOptions = new EvaluationContextOptions();
	private RedisReaderOptions readerOptions = new RedisReaderOptions();
	private ExportProcessorOptions processorOptions = new ExportProcessorOptions();

	protected <K> Function<KeyValue<K, Object>, KeyValue<K, Object>> processor(RedisCodec<K, ?> codec) {
		ToStringKeyValueFunction<K, Object> code = new ToStringKeyValueFunction<>(codec);
		StringKeyValueFunction<K, Object> decode = new StringKeyValueFunction<>(codec);
		UnaryOperator<KeyValue<String, Object>> function = keyValueOperator();
		return code.andThen(function).andThen(decode);
	}

	protected StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext evaluationContext = evaluationContextOptions.evaluationContext();
		evaluationContext.setVariable(REDIS_VAR, getRedisConnection().sync());
		return evaluationContext;
	}

	private UnaryOperator<KeyValue<String, Object>> keyValueOperator() {
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
			Function<KeyValue<String, Object>, String> function = ExpressionFunction.of(evaluationContext,
					processorOptions.getTypeExpression());
			operator.setTypeFunction(function.andThen(Type::of));
		}
		return operator;
	}

	protected abstract boolean isStruct();

	protected <K> void configureReader(String name, RedisItemReader<K, ?, ?> reader) {
		reader.setName(name);
		reader.setJobFactory(getJobFactory());
		reader.setChunkSize(readerOptions.getChunkSize());
		reader.setDatabase(getRedisURI().getDatabase());
		reader.setKeyProcessor(keyFilteringProcessor(reader.getCodec()));
		reader.setKeyPattern(readerOptions.getKeyPattern());
		if (readerOptions.getKeyType() != null) {
			reader.setKeyType(readerOptions.getKeyType().name());
		}
		reader.setFlushInterval(readerOptions.getFlushInterval());
		reader.setIdleTimeout(readerOptions.getIdleTimeout());
		reader.setNotificationQueueCapacity(readerOptions.getNotificationQueueCapacity());
		reader.setPollTimeout(readerOptions.getPollTimeout());
		reader.setQueueCapacity(readerOptions.getQueueCapacity());
		reader.setReadFrom(readerOptions.getReadFrom());
		reader.setScanCount(readerOptions.getScanCount());
		reader.setThreads(readerOptions.getThreads());
		if (reader.getOperation() instanceof KeyValueRead) {
			KeyValueRead<?, ?, ?> operation = (KeyValueRead<?, ?, ?>) reader.getOperation();
			operation.setMemUsageLimit(readerOptions.getMemoryUsageLimit());
			operation.setMemUsageSamples(readerOptions.getMemoryUsageSamples());
		}
		reader.setPoolSize(readerOptions.getPoolSize());

	}

	public <K> ItemProcessor<K, K> keyFilteringProcessor(RedisCodec<K, ?> codec) {
		Predicate<K> predicate = RiotUtils.keyFilterPredicate(codec, readerOptions.getKeyFilterOptions());
		if (predicate == null) {
			return new PassThroughItemProcessor<>();
		}
		return new PredicateItemProcessor<>(predicate);
	}

	protected <I, O> FlushingStepBuilder<I, O> flushingStep(SimpleStepBuilder<I, O> step) {
		return new FlushingStepBuilder<>(step).flushInterval(readerOptions.getFlushInterval())
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
