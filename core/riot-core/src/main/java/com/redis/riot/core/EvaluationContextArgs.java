package com.redis.riot.core;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;

import picocli.CommandLine.Option;

public class EvaluationContextArgs {

	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
	public static final String DATE_VAR = "date";

	@Option(arity = "1..*", names = "--var", description = "SpEL expressions for context variables, in the form var=\"exp\"", paramLabel = "<v=exp>")
	private Map<String, Expression> varExpressions = new LinkedHashMap<>();

	@Option(names = "--date-format", description = "Date/time format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-simple-date-format#date_time_patterns", paramLabel = "<fmt>")
	private String dateFormat = DEFAULT_DATE_FORMAT;

	private Map<String, Object> vars = new LinkedHashMap<>();

	public StandardEvaluationContext evaluationContext() {
		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.setVariable(DATE_VAR, new SimpleDateFormat(dateFormat));
		if (!CollectionUtils.isEmpty(vars)) {
			vars.forEach(evaluationContext::setVariable);
		}
		if (!CollectionUtils.isEmpty(varExpressions)) {
			varExpressions.forEach((k, v) -> evaluationContext.setVariable(k, v.getValue(evaluationContext)));
		}
		return evaluationContext;
	}

	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String format) {
		this.dateFormat = format;
	}

	public Map<String, Expression> getVarExpressions() {
		return varExpressions;
	}

	public void setVarExpressions(Map<String, Expression> expressions) {
		this.varExpressions = expressions;
	}

	public Map<String, Object> getVars() {
		return vars;
	}

	public void setVars(Map<String, Object> variables) {
		this.vars = variables;
	}

	@Override
	public String toString() {
		return "EvaluationContextArgs [varExpressions=" + varExpressions + ", dateFormat=" + dateFormat + ", vars="
				+ vars + "]";
	}

}
