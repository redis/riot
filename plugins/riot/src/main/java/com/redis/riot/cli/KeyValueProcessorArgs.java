package com.redis.riot.cli;

import org.springframework.expression.Expression;

import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.TemplateExpression;

import picocli.CommandLine.Option;

public class KeyValueProcessorArgs {

    @Option(names = "--key-proc", description = "SpEL template expression to transform the name of each key.", paramLabel = "<exp>")
    TemplateExpression keyExpression;

    @Option(names = "--type-proc", description = "SpEL expression to transform the type of each key.", paramLabel = "<exp>")
    Expression typeExpression;

    @Option(names = "--ttl-proc", description = "SpEL expression to transform the TTL of each key.", paramLabel = "<exp>")
    Expression ttlExpression;

    public KeyValueProcessorOptions processorOptions() {
        KeyValueProcessorOptions options = new KeyValueProcessorOptions();
        options.setKeyExpression(keyExpression);
        options.setTtlExpression(ttlExpression);
        options.setTypeExpression(typeExpression);
        return options;
    }

}