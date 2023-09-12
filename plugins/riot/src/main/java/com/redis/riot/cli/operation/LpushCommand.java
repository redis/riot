package com.redis.riot.cli.operation;

import com.redis.riot.core.operation.LpushBuilder;

import picocli.CommandLine.Command;

@Command(name = "lpush", description = "Insert values at the head of a list")
public class LpushCommand extends AbstractCollectionOperationCommand {

    @Override
    protected LpushBuilder collectionOperationBuilder() {
        return new LpushBuilder();
    }

}
