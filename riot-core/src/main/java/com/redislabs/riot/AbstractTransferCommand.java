package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import lombok.extern.slf4j.Slf4j;
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

    public CompletableFuture<Void> executeAsync() {
	List<Transfer<I, O>> transfers = transfers();
	ExecutionContext executionContext = new ExecutionContext();
	transfers.forEach(t -> t.open(executionContext));
	List<CompletableFuture<Void>> futures = new ArrayList<>();
	for (Transfer<I, O> transfer : transfers) {
	    futures.add(transfer.executeAsync());
	}
	CompletableFuture<Void> execution = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
	CompletableFuture<Void> progress = CompletableFuture.runAsync(new ProgressMonitor(transfers));
	execution.whenComplete((k, t) -> progress.cancel(true));
	execution.whenComplete((k, v) -> transfers.forEach(Transfer::close));
	return execution;
    }

    public List<Transfer<I, O>> transfers() {
	List<Transfer<I, O>> transfers = new ArrayList<>();
	try {
	    for (ItemReader<I> reader : readers()) {
		try {
		    transfers.add(transfer(reader));
		} catch (Exception e) {
		    log.error("Could not create transfer", e);
		}
	    }
	} catch (Exception e) {
	    log.error("Could not create readers", e);
	}
	return transfers;
    }

    private Transfer<I, O> transfer(ItemReader<I> reader) throws Exception {
	if (maxItemCount != null) {
	    if (reader instanceof AbstractItemCountingItemStreamItemReader) {
		log.debug("Configuring reader with maxItemCount={}", maxItemCount);
		((AbstractItemCountingItemStreamItemReader<I>) reader).setMaxItemCount(maxItemCount);
	    }
	}
	String readerName = name(reader);
	String name = String.format(transferNameFormat(), readerName);
	return Transfer.<I, O>builder().name(name).reader(reader).processor(processor()).writer(writer()).batch(batch)
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

    protected abstract List<ItemReader<I>> readers() throws Exception;

    protected abstract ItemProcessor<I, O> processor() throws Exception;

    protected abstract ItemWriter<O> writer() throws Exception;

}
