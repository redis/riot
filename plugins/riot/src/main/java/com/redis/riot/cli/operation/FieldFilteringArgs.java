package com.redis.riot.cli.operation;

import java.util.List;

import com.redis.riot.core.operation.AbstractFilterMapOperationBuilder;

import picocli.CommandLine.Option;

public class FieldFilteringArgs {

    @Option(arity = "1..*", names = "--include", description = "Fields to include.", paramLabel = "<field>")
    private List<String> includes;

    @Option(arity = "1..*", names = "--exclude", description = "Fields to exclude.", paramLabel = "<field>")
    private List<String> excludes;

    public void configure(AbstractFilterMapOperationBuilder<?> builder) {
        builder.includes(includes);
        builder.excludes(excludes);
    }

}
