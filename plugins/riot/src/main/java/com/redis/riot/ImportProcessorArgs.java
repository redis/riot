package com.redis.riot;

import java.util.Map;

import com.redis.riot.core.Expression;

import picocli.CommandLine.Option;

public class ImportProcessorArgs {

	@Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\" etc. For details see https://docs.spring.io/spring-framework/reference/core/expressions.html", paramLabel = "<f=exp>")
	private Map<String, Expression> expressions;

	@Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
	private Expression filter;

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
