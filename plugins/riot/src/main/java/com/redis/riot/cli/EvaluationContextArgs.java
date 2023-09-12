package com.redis.riot.cli;

import java.util.Map;

import org.springframework.expression.Expression;

import com.redis.riot.core.EvaluationContextOptions;

import picocli.CommandLine.Option;

public class EvaluationContextArgs {

    @Option(arity = "1..*", names = "--var", description = "Context variable SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
    Map<String, Expression> expressions;

    @Option(names = "--date-format", description = "Date-format pattern (default: ${DEFAULT-VALUE}). For details see https://bit.ly/javasdf", paramLabel = "<str>")
    String dateFormat = EvaluationContextOptions.DEFAULT_DATE_FORMAT;

    public EvaluationContextOptions evaluationContextOptions() {
        EvaluationContextOptions options = new EvaluationContextOptions();
        options.setExpressions(expressions);
        options.setDateFormat(dateFormat);
        return options;
    }

}
