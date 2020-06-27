package com.redislabs.riot;

import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.riot.processor.KeyValueMapItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redisearch.RediSearchAggregateCursorItemReader;
import org.springframework.batch.item.redisearch.RediSearchAggregateItemReader;
import org.springframework.batch.item.redisearch.RediSearchItemReader;
import picocli.CommandLine;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<Object, O> {

    @CommandLine.Mixin
    private RedisExportOptions options = new RedisExportOptions();
    @CommandLine.Mixin
    private RediSearchExportOptions search = new RediSearchExportOptions();
    @CommandLine.Option(names = "--key-regex", description = "Regex for key-field extraction (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String keyRegex = "\\w+:(?<id>.+)";

    @Override
    protected String taskName() {
        return "Exporting";
    }

    public ItemReader reader() {
        if (search.getIndex() == null) {
            return configure(RedisKeyValueItemReader.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threads(options.getThreads())).build();
        }
        switch (options.getFtCommand()) {
            case AGGREGATE:
                return configure(RediSearchAggregateItemReader.builder().index(search.getIndex()).query(search.getQuery()).args(search.getOptions())).build();
            case CURSOR:
                return configure(RediSearchAggregateCursorItemReader.builder().index(search.getIndex()).query(search.getQuery()).args(search.getOptions()).cursor(Cursor.builder().maxIdle(options.getCursorMaxIdle()).count(options.getCursorCount()).build())).build();
            default:
                return configure(RediSearchItemReader.builder().index(search.getIndex()).query(search.getQuery()).args(search.getOptions())).build();
        }
    }

    protected KeyValueMapItemProcessor<String, String> keyValueMapProcessor() {
        return KeyValueMapItemProcessor.builder().keyRegex(keyRegex).build();
    }

}
