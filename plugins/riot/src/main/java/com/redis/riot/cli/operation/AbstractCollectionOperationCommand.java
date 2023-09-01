package com.redis.riot.cli.operation;

import java.util.List;

import com.redis.riot.core.operation.AbstractCollectionOperationBuilder;
import com.redis.riot.core.operation.AbstractOperationBuilder;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionOperationCommand extends AbstractOperationCommand {

    @Option(names = "--member-space", description = "Keyspace prefix for member IDs.", paramLabel = "<str>")
    private String memberSpace;

    @Option(arity = "1..*", names = { "-m",
            "--members" }, description = "Member field names for collections.", paramLabel = "<fields>")
    private List<String> memberFields;

    @Override
    protected AbstractOperationBuilder<?> operationBuilder() {
        AbstractCollectionOperationBuilder<?> builder = collectionOperationBuilder();
        builder.memberSpace(memberSpace);
        builder.members(memberFields);
        return builder;
    }

    protected abstract AbstractCollectionOperationBuilder<?> collectionOperationBuilder();

}
