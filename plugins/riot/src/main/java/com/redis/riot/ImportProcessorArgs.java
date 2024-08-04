package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.expression.EvaluationContext;
import org.springframework.util.CollectionUtils;

import com.redis.riot.AbstractImportCommand.ExpressionProcessor;
import com.redis.riot.core.Expression;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.core.processor.PredicateOperator;

import picocli.CommandLine.Option;

public class ImportProcessorArgs {

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\" etc. For details see https://docs.spring.io/spring-framework/reference/core/expressions.html", paramLabel = "<f=exp>")
	private Map<String, Expression> expressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	private Expression filter;

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(EvaluationContext context) {
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (filter != null) {
			processors.add(new FunctionItemProcessor<>(new PredicateOperator<>(filter.predicate(context))));
		}
		if (!CollectionUtils.isEmpty(expressions)) {
			processors.add(new ExpressionProcessor(context, expressions));
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
		return "ImportProcessorArgs [expressions=" + expressions + ", filter=" + filter + "]";
	}

}
