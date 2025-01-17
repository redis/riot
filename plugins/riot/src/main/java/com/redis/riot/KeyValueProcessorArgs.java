package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.Expression;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.core.processor.ConsumerUnaryOperator;
import com.redis.riot.function.StreamItemProcessor;
import com.redis.spring.batch.item.redis.common.KeyValue;

import lombok.ToString;
import picocli.CommandLine.Option;

@ToString
public class KeyValueProcessorArgs {

	@Option(names = "--key-proc", description = "SpEL template expression to transform key names, e.g. \"#{#source.database}:#{key}\" for 'abc' returns '0:abc'.", paramLabel = "<exp>")
	private TemplateExpression keyExpression;

	@Option(names = "--type-proc", description = "SpEL expression to transform key types.", paramLabel = "<exp>")
	private Expression typeExpression;

	@Option(names = "--ttl-proc", description = "SpEL expression to transform key expiration times.", paramLabel = "<exp>")
	private Expression ttlExpression;

	@Option(names = "--ttl", description = "Propagate key expiration times. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean propagateTtl = true;

	@Option(names = "--stream-id", description = "Propagate stream message IDs. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean propagateIds = true;

	@Option(names = "--stream-prune", description = "Drop empty streams.")
	private boolean prune;

	public ItemProcessor<KeyValue<String>, KeyValue<String>> processor(EvaluationContext context) {
		List<ItemProcessor<KeyValue<String>, KeyValue<String>>> processors = new ArrayList<>();
		if (keyExpression != null) {
			processors.add(processor(t -> t.setKey(keyExpression.getValue(context, t))));
		}
		if (!propagateTtl) {
			processors.add(processor(t -> t.setTtl(0)));
		}
		if (ttlExpression != null) {
			processors.add(processor(t -> t.setTtl(ttlExpression.getLong(context, t))));
		}
		if (typeExpression != null) {
			processors.add(processor(t -> t.setType(typeExpression.getString(context, t))));
		}
		if (!propagateIds || prune) {
			StreamItemProcessor streamProcessor = new StreamItemProcessor();
			streamProcessor.setDropMessageIds(!propagateIds);
			streamProcessor.setPrune(prune);
			processors.add(streamProcessor);
		}
		return RiotUtils.processor(processors);
	}

	private <T> ItemProcessor<T, T> processor(Consumer<T> consumer) {
		return new FunctionItemProcessor<>(new ConsumerUnaryOperator<>(consumer));
	}

	public TemplateExpression getKeyExpression() {
		return keyExpression;
	}

	public void setKeyExpression(TemplateExpression expression) {
		this.keyExpression = expression;
	}

	public Expression getTypeExpression() {
		return typeExpression;
	}

	public void setTypeExpression(Expression expression) {
		this.typeExpression = expression;
	}

	public Expression getTtlExpression() {
		return ttlExpression;
	}

	public void setTtlExpression(Expression expression) {
		this.ttlExpression = expression;
	}

	public boolean isPropagateTtl() {
		return propagateTtl;
	}

	public void setPropagateTtl(boolean propagate) {
		this.propagateTtl = propagate;
	}

	public boolean isPropagateIds() {
		return propagateIds;
	}

	public void setPropagateIds(boolean propagateIds) {
		this.propagateIds = propagateIds;
	}

	public boolean isPrune() {
		return prune;
	}

	public void setPrune(boolean prune) {
		this.prune = prune;
	}

}
