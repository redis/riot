package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemWriter;

public abstract class AbstractImportCommand<I> extends AbstractTransferCommand<I, Object> {

    @Override
    protected ItemWriter<Object> writer() {
        return getParentCommand().writer();
    }

    protected ImportCommand getParentCommand() {
        return (ImportCommand) parentCommand;
    }
}
