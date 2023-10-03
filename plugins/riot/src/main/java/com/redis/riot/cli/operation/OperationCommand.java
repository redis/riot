package com.redis.riot.cli.operation;

import java.util.List;
import java.util.Map;

import com.redis.riot.cli.BaseCommand;
import com.redis.riot.core.operation.AbstractMapOperationBuilder;
import com.redis.spring.batch.writer.WriteOperation;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(mixinStandardHelpOptions = true)
public abstract class OperationCommand extends BaseCommand {

    @Option(names = { "-p", "--keyspace" }, description = "Keyspace prefix.", paramLabel = "<str>")
    private String keyspace;

    @Option(names = { "-k", "--keys" }, arity = "1..*", description = "Key fields.", paramLabel = "<fields>")
    private List<String> keys;

    @Option(names = { "-s", "--separator" }, description = "Key separator (default: ${DEFAULT-VALUE}).", paramLabel = "<str>")
    private String keySeparator = AbstractMapOperationBuilder.DEFAULT_SEPARATOR;

    @Option(names = { "-r", "--remove" }, description = "Remove key or member fields the first time they are used.")
    private boolean removeFields = AbstractMapOperationBuilder.DEFAULT_REMOVE_FIELDS;

    @Option(names = "--ignore-missing", description = "Ignore missing fields.")
    private boolean ignoreMissingFields = AbstractMapOperationBuilder.DEFAULT_IGNORE_MISSING_FIELDS;

    public WriteOperation<String, String, Map<String, Object>> operation() {
        AbstractMapOperationBuilder builder = operationBuilder();
        builder.setIgnoreMissingFields(ignoreMissingFields);
        builder.setKeyFields(keys);
        builder.setKeySeparator(keySeparator);
        builder.setKeyspace(keyspace);
        builder.setRemoveFields(removeFields);
        return builder.build();
    }

    protected abstract AbstractMapOperationBuilder operationBuilder();

}
