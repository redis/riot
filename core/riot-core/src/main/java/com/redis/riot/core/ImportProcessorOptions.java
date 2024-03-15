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
import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.TypedValue;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.util.GeoLocation;
import com.redis.riot.core.function.ExpressionFunction;
import com.redis.riot.core.function.MapFunction;

public class ImportProcessorOptions {

	private Map<String, Expression> processorExpressions;
	private Expression filterExpression;

	public Map<String, Expression> getProcessorExpressions() {
		return processorExpressions;
	}

	public void setProcessorExpressions(Map<String, Expression> expressions) {
		this.processorExpressions = expressions;
	}

	public Expression getFilterExpression() {
		return filterExpression;
	}

	public void setFilterExpression(Expression filter) {
		this.filterExpression = filter;
	}

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(
			StandardEvaluationContext evaluationContext) {
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		try {
			evaluationContext.registerFunction("geo",
					GeoLocation.class.getDeclaredMethod("toString", String.class, String.class));
		} catch (NoSuchMethodException e) {
			// ignore
		}
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (!CollectionUtils.isEmpty(processorExpressions)) {
			Map<String, Function<Map<String, Object>, Object>> functions = new LinkedHashMap<>();
			for (Entry<String, Expression> field : processorExpressions.entrySet()) {
				functions.put(field.getKey(),
						new ExpressionFunction<>(evaluationContext, field.getValue(), Object.class));
			}
			processors.add(new FunctionItemProcessor<>(new MapFunction(functions)));
		}
		if (filterExpression != null) {
			Predicate<Map<String, Object>> predicate = RiotUtils.predicate(evaluationContext, filterExpression);
			processors.add(new PredicateItemProcessor<>(predicate));
		}
		return RiotUtils.processor(processors);
	}

	/**
	 * {@link org.springframework.context.expression.MapAccessor} that always
	 * returns true for canRead and does not throw AccessExceptions
	 *
	 */
	private static class QuietMapAccessor extends MapAccessor {

		@Override
		public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
			return true;
		}

		@Override
		public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {
			try {
				return super.read(context, target, name);
			} catch (AccessException e) {
				return new TypedValue(null);
			}
		}

	}

}
