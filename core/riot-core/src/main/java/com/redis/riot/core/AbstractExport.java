package com.redis.riot.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.function.CompositeOperator;
import com.redis.riot.core.function.DropStreamMessageId;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.LongExpressionFunction;
import com.redis.riot.core.function.StringKeyValueFunction;
import com.redis.riot.core.function.ToStringKeyValueFunction;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.util.BatchUtils;
import com.redis.spring.batch.util.Predicates;

import io.lettuce.core.codec.RedisCodec;

public abstract class AbstractExport extends AbstractRedisCallable {

	private RedisReaderOptions readerOptions = new RedisReaderOptions();
	private KeyFilterOptions keyFilterOptions = new KeyFilterOptions();
	private ExportProcessorOptions processorOptions = new ExportProcessorOptions();

	protected <K> FunctionItemProcessor<KeyValue<K, Object>, KeyValue<K, Object>> processor(RedisCodec<K, ?> codec) {
		if (processorOptions.isEmpty()) {
			return null;
		}
		Function<KeyValue<K, Object>, KeyValue<String, Object>> code = new ToStringKeyValueFunction<>(codec);
		Function<KeyValue<String, Object>, KeyValue<K, Object>> decode = new StringKeyValueFunction<>(codec);
		CompositeOperator<KeyValue<String, Object>> operator = new CompositeOperator<>(processorConsumers());
		return new FunctionItemProcessor<>(code.andThen(operator).andThen(decode));
	}

	private List<Consumer<KeyValue<String, Object>>> processorConsumers() {
		List<Consumer<KeyValue<String, Object>>> consumers = new ArrayList<>();
		if (processorOptions.getKeyExpression() != null) {
			ExpressionFunction<Object, String> function = expressionFunction(
					processorOptions.getKeyExpression().getExpression());
			consumers.add(t -> t.setKey(function.apply(t)));
		}
		if (processorOptions.isDropTtl()) {
			consumers.add(t -> t.setTtl(0));
		}
		if (processorOptions.getTtlExpression() != null) {
			LongExpressionFunction<Object> function = longExpressionFunction(processorOptions.getTtlExpression());
			consumers.add(t -> t.setTtl(function.applyAsLong(t)));
		}
		if (processorOptions.isDropStreamMessageId() && isStruct()) {
			consumers.add(new DropStreamMessageId());
		}
		if (processorOptions.getTypeExpression() != null) {
			ExpressionFunction<KeyValue<String, Object>, String> function = expressionFunction(
					processorOptions.getTypeExpression());
			consumers.add(t -> t.setType(function.apply(t)));
		}
		return consumers;
	}

	protected abstract boolean isStruct();

	@Override
	protected <K, V, T> void configure(RedisItemReader<K, V, T> reader) {
		reader.setJobFactory(getJobFactory());
		reader.setDatabase(redisURI.getDatabase());
		if (!keyFilterOptions.isEmpty()) {
			Predicate<K> predicate = keyFilterPredicate(reader.getCodec());
			reader.setKeyProcessor(new PredicateItemProcessor<>(predicate));
		}
		readerOptions.configure(reader);
		super.configure(reader);
	}

	public <K> Predicate<K> keyFilterPredicate(RedisCodec<K, ?> codec) {
		return slotsPredicate(codec).and(globPredicate(codec));
	}

	private <K> Predicate<K> slotsPredicate(RedisCodec<K, ?> codec) {
		if (CollectionUtils.isEmpty(keyFilterOptions.getSlots())) {
			return Predicates.isTrue();
		}
		Stream<Predicate<K>> predicates = keyFilterOptions.getSlots().stream()
				.map(r -> Predicates.slotRange(codec, r.getStart(), r.getEnd()));
		return Predicates.or(predicates);
	}

	private <K> Predicate<K> globPredicate(RedisCodec<K, ?> codec) {
		return Predicates.map(BatchUtils.toStringKeyFunction(codec), globPredicate());
	}

	private Predicate<String> globPredicate() {
		Predicate<String> include = RiotUtils.globPredicate(keyFilterOptions.getIncludes());
		if (CollectionUtils.isEmpty(keyFilterOptions.getExcludes())) {
			return include;
		}
		return include.and(RiotUtils.globPredicate(keyFilterOptions.getExcludes()).negate());
	}

	public RedisReaderOptions getReaderOptions() {
		return readerOptions;
	}

	public void setReaderOptions(RedisReaderOptions options) {
		this.readerOptions = options;
	}

	public ExportProcessorOptions getProcessorOptions() {
		return processorOptions;
	}

	public void setProcessorOptions(ExportProcessorOptions options) {
		this.processorOptions = options;
	}

	public KeyFilterOptions getKeyFilterOptions() {
		return keyFilterOptions;
	}

	public void setKeyFilterOptions(KeyFilterOptions options) {
		this.keyFilterOptions = options;
	}

}
