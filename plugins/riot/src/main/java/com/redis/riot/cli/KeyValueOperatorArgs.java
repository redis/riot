package com.redis.riot.cli;

import org.springframework.expression.Expression;

import com.redis.riot.core.KeyValueOperatorOptions;
import com.redis.riot.core.TemplateExpression;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class KeyValueOperatorArgs {

    @Option(names = "--key-proc", description = "SpEL template expression to transform the name of each key.", paramLabel = "<exp>")
    private TemplateExpression keyExpression;

    @Option(names = "--type-proc", description = "SpEL template expression to transform the type of each key.", paramLabel = "<exp>")
    private TemplateExpression typeExpression;

    @Option(names = "--ttl-proc", description = "SpEL expression to transform the TTL of each key.", paramLabel = "<exp>")
    private Expression ttlExpression;

    @ArgGroup(exclusive = false)
    private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

    public KeyValueOperatorOptions keyValueOperatorOptions() {
        KeyValueOperatorOptions options = new KeyValueOperatorOptions();
        options.setKeyExpression(keyExpression);
        options.setTtlExpression(ttlExpression);
        options.setTypeExpression(typeExpression);
        options.setEvaluationContextOptions(evaluationContextArgs.evaluationContextOptions());
        return options;
    }

}
