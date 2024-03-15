package com.redis.riot.cli;

import java.util.Map;

import org.springframework.expression.Expression;

import com.redis.riot.core.EvaluationContextOptions;

import picocli.CommandLine.Option;

public class EvaluationContextArgs {

	@Option(arity = "1..*", names = "--var", description = "SpEL expressions for context variables, in the form var=\"exp\"", paramLabel = "<v=exp>")
	Map<String, Expression> variableExpressions;

	@Option(names = "--date-format", description = "Date/time format (default: ${DEFAULT-VALUE}). For details see https://www.baeldung.com/java-simple-date-format#date_time_patterns", paramLabel = "<fmt>")
	String dateFormat = EvaluationContextOptions.DEFAULT_DATE_FORMAT;

	EvaluationContextOptions evaluationContextOptions() {
		EvaluationContextOptions options = new EvaluationContextOptions();
		options.setVarExpressions(variableExpressions);
		options.setDateFormat(dateFormat);
		return options;
	}

}
