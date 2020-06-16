package com.redislabs.riot.cli;

import com.redislabs.lettuce.helper.RedisOptions;
import com.redislabs.riot.Transfer;
import com.redislabs.riot.TransferOptions;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import picocli.CommandLine;

public abstract class TransferCommand extends RiotCommand {

    @CommandLine.Option(names = {"-t", "--threads"}, description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @CommandLine.Option(names = "--pool", description = "Max number of connections in pool (default: #threads)", paramLabel = "<int>")
    private Integer poolMaxTotal;
    @CommandLine.Option(names = {"-b", "--batch"}, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batchSize = 50;
    @CommandLine.Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;

    @Override
    public RedisOptions redisOptions() {
        RedisOptions redisOptions = super.redisOptions();
        redisOptions.getPoolOptions().setMaxTotal(getPoolMaxTotal());
        return redisOptions;
    }

    protected int getPoolMaxTotal() {
        if (this.poolMaxTotal == null) {
            return this.threads;
        }
        return this.poolMaxTotal;

    }

    public <I, O> void execute(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer) {
        Transfer<I, O> transfer = Transfer.<I, O>builder().reader(reader).processor(processor).writer(writer).options(transferOptions()).build();
        ProgressBarOptions progressBarOptions = ProgressBarOptions.builder().taskName(taskName()).initialMax(maxItemCount).quiet(isQuiet()).build();
        ProgressBarReporter reporter = ProgressBarReporter.builder().transfer(transfer).options(progressBarOptions).build();
        reporter.start();
        transfer.execute();
        reporter.stop();
    }

    protected abstract String taskName();

    protected TransferOptions transferOptions() {
        return TransferOptions.builder().batchSize(batchSize).threadCount(threads).maxItemCount(maxItemCount).build();
    }

}
