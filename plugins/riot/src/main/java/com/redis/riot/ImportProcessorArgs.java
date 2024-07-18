package com.redis.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.api.StatefulRedisModulesConnection;
import com.redis.riot.AbstractImportCommand.ExpressionProcessor;
import com.redis.riot.core.Expression;
import com.redis.riot.core.PredicateItemProcessor;
import com.redis.riot.core.QuietMapAccessor;
import com.redis.riot.core.RiotUtils;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class ImportProcessorArgs {

	@ArgGroup(exclusive = false)
	private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\" etc. For details see https://docs.spring.io/spring-framework/reference/core/expressions.html", paramLabel = "<f=exp>")
	private Map<String, Expression> expressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	private Expression filter;

	public ItemProcessor<Map<String, Object>, Map<String, Object>> processor(
			StatefulRedisModulesConnection<String, String> connection) {
		StandardEvaluationContext context = evaluationContextArgs.evaluationContext(connection);
		context.addPropertyAccessor(new QuietMapAccessor());
		List<ItemProcessor<Map<String, Object>, Map<String, Object>>> processors = new ArrayList<>();
		if (filter != null) {
			processors.add(new PredicateItemProcessor<>(filter.predicate(context)));
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

	public EvaluationContextArgs getEvaluationContextArgs() {
		return evaluationContextArgs;
	}

	public void setEvaluationContextArgs(EvaluationContextArgs evaluationContextArgs) {
		this.evaluationContextArgs = evaluationContextArgs;
	}

	@Override
	public String toString() {
		return "ImportProcessorArgs [evaluationContextArgs=" + evaluationContextArgs + ", expressions=" + expressions
				+ ", filter=" + filter + "]";
	}

}
