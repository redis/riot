package com.redislabs.riot.cli;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.picocliredis.HelpCommand;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Slf4j
@Command
public abstract class AbstractTransferCommand<I, O> extends HelpCommand implements Runnable {

    @CommandLine.ParentCommand
    protected TransferCommand parentCommand;

    @Override
    public void run() {
        ItemReader<I> reader;
        try {
            reader = reader();
        } catch (Exception e) {
            log.error("Could not create reader", e);
            return;
        }
        ItemProcessor<I, O> processor;
        try {
            processor = processor();
        } catch (Exception e) {
            log.error("Could not create processor", e);
            return;
        }
        ItemWriter writer;
        try {
            writer = writer();
        } catch (Exception e) {
            log.error("Could not create writer", e);
            return;
        }
        execute(reader, processor, writer);
    }

    protected void execute(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        parentCommand.execute(reader, processor, writer);
    }

    protected abstract ItemReader<I> reader() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected abstract ItemWriter<O> writer() throws Exception;

    protected RedisOptions redisOptions() {
        return parentCommand.redisOptions();
    }


}
