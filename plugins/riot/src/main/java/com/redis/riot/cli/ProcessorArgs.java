package com.redis.riot.cli;

import java.util.Map;

import org.springframework.expression.Expression;

import com.redis.riot.core.ProcessorOptions;

import picocli.CommandLine.Option;

public class ProcessorArgs {

    @Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
    Map<String, Expression> expressions;

    @Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
    Expression filter;

    public ProcessorOptions processorOptions() {
        ProcessorOptions options = new ProcessorOptions();
        options.setExpressions(expressions);
        options.setFilter(filter);
        return options;
    }

}
