package com.redislabs.riot.cli;

import com.redislabs.riot.processor.KeyValueItemProcessor;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisClusterKeyItemReader;
import org.springframework.batch.item.redis.RedisClusterKeyValueItemReader;
import org.springframework.batch.item.redis.RedisKeyItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.TypeKeyValue;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command
public abstract class AbstractExportCommand<O> extends com.redislabs.picocliredis.HelpCommand implements Runnable {

    @Getter
    @CommandLine.ParentCommand
    private ExportCommand parentCommand;

    protected boolean isQuiet() {
        return parentCommand.isQuiet();
    }

    @Override
    public void run() {
        ItemWriter<O> writer;
        try {
            writer = writer();
        } catch (Exception e) {
            log.error("Could not create writer", e);
            return;
        }
        parentCommand.execute("Exporting", parentCommand.reader(), processor(), writer);
    }

    protected abstract ItemProcessor<TypeKeyValue<String>, O> processor();

    protected abstract ItemWriter<O> writer() throws Exception;


}
