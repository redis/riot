package com.redislabs.riot;

import com.redislabs.riot.processor.KeyValueMapItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.KeyValue;
import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<KeyValue<String>, O> {

    @CommandLine.Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @CommandLine.Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String keyRegex = "\\w+:(?<id>.+)";

    @Override
    protected String taskName() {
        return "Exporting";
    }

    public ItemReader<KeyValue<String>> reader() {
        return configure(RedisKeyValueItemReader.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threads(options.getThreads())).build();
    }

    protected KeyValueMapItemProcessor<String, String> keyValueProcessor() {
        return KeyValueMapItemProcessor.builder().keyRegex(keyRegex).build();
    }

}
