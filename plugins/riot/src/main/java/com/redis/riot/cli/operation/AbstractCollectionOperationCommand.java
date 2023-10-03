package com.redis.riot.cli.operation;

import java.util.List;

import com.redis.riot.core.operation.AbstractCollectionMapOperationBuilder;
import com.redis.riot.core.operation.AbstractMapOperationBuilder;

import picocli.CommandLine.Option;

public abstract class AbstractCollectionOperationCommand extends OperationCommand {

    @Option(names = "--member-space", description = "Keyspace prefix for member IDs.", paramLabel = "<str>")
    private String memberSpace;

    @Option(arity = "1..*", names = { "-m",
            "--members" }, description = "Member field names for collections.", paramLabel = "<fields>")
    private List<String> memberFields;

    @Override
    protected AbstractMapOperationBuilder operationBuilder() {
        AbstractCollectionMapOperationBuilder builder = collectionOperationBuilder();
        builder.setMemberSpace(memberSpace);
        builder.setMemberFields(memberFields);
        return builder;
    }

    protected abstract AbstractCollectionMapOperationBuilder collectionOperationBuilder();

}
