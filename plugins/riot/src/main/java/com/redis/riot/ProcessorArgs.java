package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;

import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.core.Expression;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.TemplateExpression;
import com.redis.riot.function.ConsumerUnaryOperator;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ProcessorArgs {

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@Option(names = "--key-proc", description = "SpEL template expression to transform key names, e.g. \"#{#source.database}:#{key}\" for 'abc' returns '0:abc'", paramLabel = "<exp>")
	private TemplateExpression keyExpression;

	@Option(names = "--type-proc", description = "SpEL expression to transform key types.", paramLabel = "<exp>")
	private Expression typeExpression;

	@Option(names = "--ttl-proc", description = "SpEL expression to transform key expiration times.", paramLabel = "<exp>")
	private Expression ttlExpression;

	@Option(names = "--ttls", description = "Propagate key expiration times. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean propagateTtl = true;

	@ArgGroup(exclusive = false)
	private StreamProcessorArgs streamProcessorArgs = new StreamProcessorArgs();

	public ItemProcessor<KeyValue<String, Object>, KeyValue<String, Object>> keyValueProcessor(
			EvaluationContext context) {
		UnaryOperator<KeyValue<String, Object>> transform = transform(context);
		return RiotUtils.processor(streamProcessorArgs.operator(), transform);
	}

	private UnaryOperator<KeyValue<String, Object>> transform(EvaluationContext context) {
		List<Consumer<KeyValue<String, Object>>> consumers = new ArrayList<>();
		if (keyExpression != null) {
			consumers.add(t -> t.setKey(keyExpression.getValue(context, t)));
		}
		if (!propagateTtl) {
			consumers.add(t -> t.setTtl(0));
		}
		if (ttlExpression != null) {
			consumers.add(t -> t.setTtl(ttlExpression.getLong(context, t)));
		}
		if (typeExpression != null) {
			consumers.add(t -> t.setType(typeExpression.getString(context, t)));
		}
		if (consumers.isEmpty()) {
			return null;
		}
		return new ConsumerUnaryOperator<>(consumers);
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

	public StreamProcessorArgs getStreamProcessorArgs() {
		return streamProcessorArgs;
	}

	public void setStreamProcessorArgs(StreamProcessorArgs args) {
		this.streamProcessorArgs = args;
	}

	public EvaluationContextArgs getEvaluationContextArgs() {
		return evaluationContextArgs;
	}

	public void setEvaluationContextArgs(EvaluationContextArgs args) {
		this.evaluationContextArgs = args;
	}

	@Override
	public String toString() {
		return "ProcessorArgs [evaluationContextArgs=" + evaluationContextArgs + ", keyExpression=" + keyExpression
				+ ", typeExpression=" + typeExpression + ", ttlExpression=" + ttlExpression + ", propagateTtl="
				+ propagateTtl + ", streamProcessorArgs=" + streamProcessorArgs + "]";
	}

}
