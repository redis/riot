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

	@Option(names = "--no-ttl", description = "Do not propagate key expiration times.")
	private boolean noTtl;

	@Option(names = "--no-stream-id", description = "Do not propagate stream message IDs.")
	private boolean noStreamIds;

	@Option(names = "--stream-prune", description = "Drop empty streams.")
	private boolean prune;

	public ItemProcessor<KeyValue<String>, KeyValue<String>> processor(EvaluationContext context) {
		List<ItemProcessor<KeyValue<String>, KeyValue<String>>> processors = new ArrayList<>();
		if (keyExpression != null) {
			processors.add(processor(t -> t.setKey(keyExpression.getValue(context, t))));
		}
		if (noTtl) {
			processors.add(processor(t -> t.setTtl(0)));
		}
		if (ttlExpression != null) {
			processors.add(processor(t -> t.setTtl(ttlExpression.getLong(context, t))));
		}
		if (typeExpression != null) {
			processors.add(processor(t -> t.setType(typeExpression.getString(context, t))));
		}
		if (noStreamIds || prune) {
			StreamItemProcessor streamProcessor = new StreamItemProcessor();
			streamProcessor.setDropMessageIds(noStreamIds);
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

	public boolean isNoTtl() {
		return noTtl;
	}

	public void setNoTtl(boolean noTtl) {
		this.noTtl = noTtl;
	}

	public boolean isNoStreamIds() {
		return noStreamIds;
	}

	public void setNoStreamIds(boolean noStreamIds) {
		this.noStreamIds = noStreamIds;
	}

	public boolean isPrune() {
		return prune;
	}

	public void setPrune(boolean prune) {
		this.prune = prune;
	}

}
