package com.redis.riot.cli;

import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.expression.Expression;

import com.redis.riot.core.MapProcessorOptions;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class MapProcessorArgs {

    @Option(arity = "1..*", names = "--proc", description = "SpEL expressions in the form field1=\"exp\" field2=\"exp\"...", paramLabel = "<f=exp>")
    private Map<String, Expression> expressions;

    @Option(arity = "1..*", names = "--regex", description = "Regular expressions in the form field1=\"regex\" field2=\"regex\"...", paramLabel = "<f=rex>")
    private Map<String, Pattern> regexes;

    @Option(names = "--filter", description = "Discard records using a SpEL expression.", paramLabel = "<exp>")
    private Expression filter;

    @ArgGroup(exclusive = false)
    private EvaluationContextArgs evaluationContextArgs = new EvaluationContextArgs();

    public MapProcessorOptions processorOptions() {
        MapProcessorOptions options = new MapProcessorOptions();
        options.setEvaluationContextOptions(evaluationContextArgs.evaluationContextOptions());
        options.setExpressions(expressions);
        options.setFilter(filter);
        options.setRegexes(regexes);
        return options;
    }

}
