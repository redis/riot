package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.support.Transfer;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import picocli.CommandLine.Option;

@Slf4j
public abstract class AbstractTransferCommand<I, O> extends RiotCommand {

    @Option(names = "--threads", description = "Thread count (default: ${DEFAULT-VALUE})", paramLabel = "<int>")
    private int threads = 1;
    @Option(names = { "-b",
	    "--batch" }, description = "Number of items in each batch (default: ${DEFAULT-VALUE})", paramLabel = "<size>")
    private int batch = 50;
    @Option(names = "--max", description = "Max number of items to read", paramLabel = "<count>")
    private Integer maxItemCount;

    @Override
    public void run() {
	try {
	    executeAsync().join();
	} catch (Exception e) {
	    log.error("Could not execute command", e);
	}
    }

    public CompletableFuture<Void> executeAsync() throws Exception {
	List<CompletableFuture<Void>> futures = new ArrayList<>();
	for (Transfer<I, O> transfer : transfers()) {
	    CompletableFuture<Void> future = transfer.executeAsync();
	    ProgressBarBuilder builder = new ProgressBarBuilder();
	    int available = transfer.available();
	    if (available > 0) {
		builder.setInitialMax(available);
	    }
	    builder.setTaskName(transfer.getName());
	    builder.showSpeed();
	    ProgressBar progressBar = builder.build();
	    transfer.addListener(progressBar::stepTo);
	    future.whenComplete((k, t) -> progressBar.close());
	    futures.add(future);
	}
	return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }

    protected abstract List<Transfer<I, O>> transfers() throws Exception;

    protected Transfer<I, O> transfer(ItemReader<I> reader, ItemProcessor<I, O> processor, ItemWriter<O> writer)
	    throws Exception {
	if (maxItemCount != null) {
	    if (reader instanceof AbstractItemCountingItemStreamItemReader) {
		log.debug("Configuring reader with maxItemCount={}", maxItemCount);
		((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
	    }
	}
	String readerName = name(reader);
	String name = String.format(transferNameFormat(), readerName);
	return Transfer.<I, O>builder().name(name).reader(reader).processor(processor).writer(writer).batch(batch)
		.threads(threads).build();
    }

    protected abstract String transferNameFormat();

    private String name(ItemReader<I> reader) {
	if (reader instanceof ItemStreamSupport) {
	    // this is a hack to get the source name
	    String name = ((ItemStreamSupport) reader).getExecutionContextKey("");
	    return name.substring(0, name.length() - 1);
	}
	return ClassUtils.getShortName(reader.getClass());

    }

}
