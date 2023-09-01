package com.redis.riot.cli.operation;

import com.redis.riot.core.operation.DelOperationBuilder;

import picocli.CommandLine.Command;

@Command(name = "del", description = "Delete keys")
public class DelCommand extends AbstractOperationCommand {

    @Override
    protected DelOperationBuilder operationBuilder() {
        return new DelOperationBuilder();
    }

}
