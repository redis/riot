package com.redis.riot.cli.operation;

import com.redis.riot.core.operation.DelBuilder;

import picocli.CommandLine.Command;

@Command(name = "del", description = "Delete keys")
public class DelCommand extends OperationCommand {

    @Override
    protected DelBuilder operationBuilder() {
        return new DelBuilder();
    }

}
