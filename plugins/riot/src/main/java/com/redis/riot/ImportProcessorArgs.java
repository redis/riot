package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.EvaluationContextArgs;
import com.redis.riot.core.FilterFunction;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.function.ConsumerUnaryOperator;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ImportProcessorArgs {

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@ArgGroup(exclusive = false)
	private KeyValueProcessorArgs keyValueProcessorArgs = new KeyValueProcessorArgs();

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
	private Map<String, Expression> expressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	private Expression filter;

	@SuppressWarnings("unchecked")
	public ItemProcessor<Map<String, Object>, Map<String, Object>> mapProcessor(EvaluationContext context) {
		List<UnaryOperator<Map<String, Object>>> functions = new ArrayList<>();
		if (!CollectionUtils.isEmpty(expressions)) {
			functions.add(new ConsumerUnaryOperator<>(t -> putAll(t, context)));
		}
		if (filter != null) {
			functions.add(new FilterFunction<>(RiotUtils.predicate(context, filter)));
		}
		return RiotUtils.processor(functions);
	}

	private void putAll(Map<String, Object> map, EvaluationContext context) {
		expressions.forEach((k, v) -> map.put(k, v.getValue(context, map, Object.class)));
	}

	public EvaluationContextArgs getEvaluationContextArgs() {
		return evaluationContextArgs;
	}

	public void setEvaluationContextArgs(EvaluationContextArgs args) {
		this.evaluationContextArgs = args;
	}

	public KeyValueProcessorArgs getKeyValueProcessorArgs() {
		return keyValueProcessorArgs;
	}

	public void setKeyValueProcessorArgs(KeyValueProcessorArgs args) {
		this.keyValueProcessorArgs = args;
	}

	public Map<String, Expression> getExpressions() {
		return expressions;
	}

	public void setExpressions(Map<String, Expression> expressions) {
		this.expressions = expressions;
	}

	public Expression getFilter() {
		return filter;
	}

	public void setFilter(Expression filter) {
		this.filter = filter;
	}

	@Override
	public String toString() {
		return "ImportProcessorArgs [evaluationContextArgs=" + evaluationContextArgs + ", keyValueProcessorArgs="
				+ keyValueProcessorArgs + ", expressions=" + expressions + ", filter=" + filter + "]";
	}

}
