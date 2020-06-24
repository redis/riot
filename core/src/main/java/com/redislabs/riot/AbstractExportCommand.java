package com.redislabs.riot;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import picocli.CommandLine;

@CommandLine.Command
public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<KeyValue<String>, O> {

    @CommandLine.Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @CommandLine.Option(names = "--key-regex", description = "Regular expression for key-field extraction", paramLabel = "<regex>")
    private String keyRegex;

    @Override
    protected String taskName() {
        return "Exporting";
    }

    public ItemReader<KeyValue<String>> reader() {
        return configure(RedisKeyValueItemReader.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threads(options.getThreads())).build();
    }

}
