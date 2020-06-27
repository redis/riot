package com.redislabs.riot;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.batch.item.redisearch.support.RediSearchConnectionBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.PassThroughItemProcessor;
import picocli.CommandLine;

import java.util.List;

@Slf4j
@CommandLine.Command(abbreviateSynopsis = true, sortOptions = false)
public abstract class AbstractTransferCommand<I, O> extends HelpCommand implements Runnable {

    @CommandLine.ParentCommand
    private RiotApp app;
    @CommandLine.Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @CommandLine.Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;

    protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder) {
        return app.configure(builder);
    }

    protected <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder) {
        return app.configure(builder);
    }

    protected <B extends RedisConnectionBuilder<B>> B configure(RedisConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return app.configure(builder, redis);
    }

    protected <B extends RediSearchConnectionBuilder<B>> B configure(RediSearchConnectionBuilder<B> builder, RedisConnectionOptions redis) {
        return app.configure(builder, redis);
    }

    protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        return Transfer.<I, O>builder().reader(reader).processor(processor).writer(writer).batchSize(batchSize).threadCount(threads).maxItemCount(maxItemCount).flushPeriod(flushPeriod()).build();
    }

    protected Long flushPeriod() {
        return null;
    }

    protected abstract String taskName();

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void run() {
        ItemReader<I> reader;
        try {
            reader = reader();
        } catch (Exception e) {
            log.error("Could not create reader", e);
            return;
        }
        ItemWriter writer;
        try {
            writer = writer();
        } catch (Exception e) {
            log.error("Could not create writer", e);
            return;
        }
        ItemProcessor<I, O> processor = processor();
        Transfer<I, O> transfer = transfer(reader, processor, writer);
        transfer.addListener(ProgressBarReporter.builder().transfer(transfer).taskName(taskName()).initialMax(maxItemCount).quiet(app.isQuiet()).build());
        try {
            transfer.open();
        } catch (Exception e) {
            log.error("Could not start transfer", e);
            return;
        }
        try {
            transfer.execute();
        } catch (Exception e) {
            log.error("Could not execute transfer", e);
        } finally {
            transfer.close();
        }
    }

    protected abstract ItemReader<I> reader() throws Exception;

    protected abstract ItemProcessor<I, O> processor();

    protected abstract ItemWriter<O> writer() throws Exception;

    protected ItemProcessor compositeProcessor(List<ItemProcessor> allProcessors) {
        if (allProcessors.isEmpty()) {
            return new PassThroughItemProcessor();
        }
        if (allProcessors.size() == 1) {
            return allProcessors.get(0);
        }
        CompositeItemProcessor compositeItemProcessor = new CompositeItemProcessor();
        compositeItemProcessor.setDelegates(allProcessors);
        return compositeItemProcessor;
    }


}
