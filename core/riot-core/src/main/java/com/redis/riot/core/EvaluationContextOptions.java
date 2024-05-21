package com.redis.riot.core;

import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.CollectionUtils;

import com.redis.lettucemod.util.GeoLocation;

public class EvaluationContextOptions {

	public static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";

	private static final String DEFAULT_DATE_VAR = "date";

	private String dateVar = DEFAULT_DATE_VAR;
	private String dateFormat = DEFAULT_DATE_FORMAT;
	private Map<String, Expression> varExpressions = new LinkedHashMap<>();
	private Map<String, Object> vars = new LinkedHashMap<>();

	public String getDateVar() {
		return dateVar;
	}

	public void setDateVar(String dateVar) {
		this.dateVar = dateVar;
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

	public StandardEvaluationContext evaluationContext() throws NoSuchMethodException, SecurityException {
		StandardEvaluationContext evaluationContext = new StandardEvaluationContext();
		evaluationContext.registerFunction("geo",
				GeoLocation.class.getDeclaredMethod("toString", String.class, String.class));
		evaluationContext.setVariable(dateVar, new SimpleDateFormat(dateFormat));
		if (!CollectionUtils.isEmpty(vars)) {
			vars.forEach(evaluationContext::setVariable);
		}
		if (!CollectionUtils.isEmpty(varExpressions)) {
			varExpressions.forEach((k, v) -> evaluationContext.setVariable(k, v.getValue(evaluationContext)));
		}
		return evaluationContext;
	}

}
