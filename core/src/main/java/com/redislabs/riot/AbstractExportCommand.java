package com.redislabs.riot;

import com.redislabs.lettusearch.aggregate.Cursor;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.suggest.Suggestion;
import com.redislabs.lettusearch.suggest.SuggetOptions;
import com.redislabs.riot.processor.DocumentMapItemProcessor;
import com.redislabs.riot.processor.DocumentWritableProcessor;
import com.redislabs.riot.processor.KeyValueMapItemProcessor;
import com.redislabs.riot.processor.SuggestionMapItemProcessor;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisKeyValueItemReader;
import org.springframework.batch.item.redisearch.RediSearchAggregateCursorItemReader;
import org.springframework.batch.item.redisearch.RediSearchAggregateItemReader;
import org.springframework.batch.item.redisearch.RediSearchItemReader;
import org.springframework.batch.item.redisearch.RediSearchSuggestItemReader;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.util.Map;

@SuppressWarnings("rawtypes")
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
        if (search.getIndex() != null) {
            switch (search.getReader()) {
                case AGGREGATE:
                    return configure(RediSearchAggregateItemReader.builder().index(search.getIndex()).query(search.getQuery()).args(search.getArgs())).build();
                case CURSOR:
                    return configure(RediSearchAggregateCursorItemReader.builder().index(search.getIndex()).query(search.getQuery()).args(search.getArgs()).cursor(Cursor.builder().maxIdle(options.getCursorMaxIdle()).count(options.getCursorCount()).build())).build();
                case SUGGEST:
                    return configure(RediSearchSuggestItemReader.builder().key(search.getIndex()).prefix(search.getQuery()).suggetOptions(SuggetOptions.builder().build())).build();
                default:
                    return configure(RediSearchItemReader.builder().index(search.getIndex()).query(search.getQuery()).args(search.getArgs()).searchOptions(SearchOptions.builder().withScores(search.isWithScores()).withPayloads(search.isWithPayloads()).limit(Limit.builder().num(search.getMax()).build()).build())).build();
            }
        }
        return configure(RedisKeyValueItemReader.builder().scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getBatchSize()).queueCapacity(options.getQueueCapacity()).threads(options.getThreads())).build();
    }

    protected ItemProcessor targetObjectProcessor() {
        if (search.getIndex()!=null) {
            if (search.getReader()== RediSearchExportOptions.RediSearchReader.SEARCH) {
                return new DocumentWritableProcessor();
            }
        }
        return new PassThroughItemProcessor();
    }

    protected Class targetClass() {
        if (search.getIndex() != null) {
            switch (search.getReader()) {
                case AGGREGATE:
                case CURSOR:
                    return Map.class;
                case SUGGEST:
                    return Suggestion.class;
                default:
                    return WritableDocument.class;
            }
        }
        return KeyValue.class;
    }

    protected ItemProcessor mapProcessor() {
        if (search.getIndex() != null) {
            switch (search.getReader()) {
                case AGGREGATE:
                case CURSOR:
                    return new PassThroughItemProcessor();
                case SUGGEST:
                    return new SuggestionMapItemProcessor();
                default:
                    return new DocumentMapItemProcessor();
            }
        }
        return KeyValueMapItemProcessor.builder().keyRegex(keyRegex).build();
    }

}
