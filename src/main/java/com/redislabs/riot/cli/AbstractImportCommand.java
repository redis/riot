package com.redislabs.riot.cli;

import com.redislabs.picocliredis.RedisOptions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@SuppressWarnings({"rawtypes", "unchecked"})
@Slf4j
@Command
public abstract class AbstractImportCommand<I> extends com.redislabs.picocliredis.HelpCommand implements Runnable {

    @Getter
    @CommandLine.ParentCommand
    private ImportCommand parentCommand;

    protected RedisOptions getRedisOptions() {
        return parentCommand.getRedisOptions();
    }

    protected boolean isQuiet() {
        return parentCommand.isQuiet();
    }

    @Override
    public void run() {
        ItemReader<I> reader;
        try {
            reader = reader();
        } catch (Exception e) {
            log.error("Could not create reader", e);
            return;
        }
        parentCommand.execute("Importing", reader, processor(), parentCommand.writer());
    }

    protected abstract ItemReader<I> reader() throws Exception;

    protected abstract ItemProcessor<I, Object> processor();


}
