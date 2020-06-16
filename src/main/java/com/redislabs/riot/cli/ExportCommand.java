package com.redislabs.riot.cli;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.KeyValue;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redis.support.QueueOptions;
import org.springframework.batch.item.redis.support.ReaderOptions;
import picocli.CommandLine;

@CommandLine.Command(name = "export", description = "Export data from Redis", subcommands = {FileExportCommand.class, DatabaseExportCommand.class})
public class ExportCommand extends TransferCommand {

    @CommandLine.Mixin
    private ExportOptions exportOptions = new ExportOptions();
    @CommandLine.Option(names = "--key-regex", description = "Regular expression for key-field extraction", paramLabel = "<regex>")
    private String keyRegex;

    @Override
    protected String taskName() {
        return "Exporting";
    }

    public ItemReader<KeyValue<String>> reader() {
        ReaderOptions readerOptions = ReaderOptions.builder().scanCount(exportOptions.getScanCount()).scanMatch(exportOptions.getScanMatch()).batchSize(exportOptions.getBatchSize()).valueQueueOptions(QueueOptions.builder().capacity(exportOptions.getQueueCapacity()).build()).threadCount(exportOptions.getThreads()).build();
        return RedisKeyValueItemReader.builder().redisOptions(redisOptions()).readerOptions(readerOptions).build();
    }

}
