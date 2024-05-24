package com.redis.riot.core;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Predicate;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.util.CollectionUtils;

import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.MapFunction;

import picocli.CommandLine.Option;

public class MapProcessorArgs {

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
	private Map<String, Expression> expressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	private Expression filter;

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(EvaluationContext context) {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (!CollectionUtils.isEmpty(expressions)) {
			Map<String, Function<Map<String, Object>, Object>> functions = new LinkedHashMap<>();
			for (Entry<String, Expression> field : expressions.entrySet()) {
				functions.put(field.getKey(), new ExpressionFunction<>(context, field.getValue(), Object.class));
			}
			processors.add(new FunctionItemProcessor<>(new MapFunction(functions)));
		}
		if (filter != null) {
			Predicate<Map<String, Object>> predicate = RiotUtils.predicate(context, filter);
			processors.add(new PredicateItemProcessor<>(predicate));
		}
		return RiotUtils.processor(processors);
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
		return "MapProcessorArgs [expressions=" + expressions + ", filter=" + filter + "]";
	}

}
