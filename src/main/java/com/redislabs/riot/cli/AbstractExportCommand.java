package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.KeyValue;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<KeyValue<String>, O> {

    protected ExportCommand getParentCommand() {
        return (ExportCommand) parentCommand;
    }

    @Override
    protected ItemReader<KeyValue<String>> reader() {
        return getParentCommand().reader();
    }
}
