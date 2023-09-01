package com.redis.riot.cli.operation;

import java.util.List;
import java.util.Map;

import com.redis.riot.core.operation.AbstractOperationBuilder;
import com.redis.riot.core.operation.OperationBuilder;
import com.redis.spring.batch.writer.Operation;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(mixinStandardHelpOptions = true)
public abstract class AbstractOperationCommand implements OperationBuilder {

    @Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix.", paramLabel = "<str>")
    private String keyspace;

    @Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields.", paramLabel = "<fields>")
    private List<String> keys;

    @Option(names = { "-s", "--separator" }, description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String keySeparator = AbstractOperationBuilder.DEFAULT_SEPARATOR;

    @Option(names = { "-r", "--remove" }, description = "Remove key or member fields the first time they are used.")
    private boolean removeFields = AbstractOperationBuilder.DEFAULT_REMOVE_FIELDS;

    @Option(names = "--ignore-missing", description = "Ignore missing fields.")
    private boolean ignoreMissingFields = AbstractOperationBuilder.DEFAULT_IGNORE_MISSING_FIELDS;

    @Override
    public Operation<String, String, Map<String, Object>> build() {
        AbstractOperationBuilder<?> builder = operationBuilder();
        builder.ignoreMissingFields(ignoreMissingFields);
        builder.keys(keys);
        builder.keySeparator(keySeparator);
        builder.keyspace(keyspace);
        builder.removeFields(removeFields);
        return builder.build();
    }

    protected abstract AbstractOperationBuilder<?> operationBuilder();

}
